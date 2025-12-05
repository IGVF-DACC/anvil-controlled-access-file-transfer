import asyncio

import os

import time

from typing import Dict, List, Set, Any

import requests

from google.cloud.storage import Client as StorageClient

from google.cloud.storage_transfer import StorageTransferServiceClient
from google.cloud.storage_transfer import TransferJob

from google.protobuf.json_format import MessageToDict

from datetime import datetime

from google.oauth2 import service_account

from dataclasses import dataclass

import logging

import argparse

from igvf_async_client import AsyncIgvfApi

from metadata import collect_metadata
from metadata import MetadataProps
from metadata import make_sts_manifests_from_metadata
from metadata import make_data_tables
from metadata import get_session
from metadata import post_tsv_from_memory
from metadata import delete_table_named
from metadata import get_name_from_tsv

from cache import PortalCacheProps
from cache import PortalCache

parser = argparse.ArgumentParser()

parser.add_argument(
    '--log-level',
    default='INFO',
    choices=[
        'DEBUG',
        'INFO',
        'WARNING',
        'ERROR',
        'CRITICAL'
    ],
    help='Set the logging level'
)

parser.add_argument(
    '--dul',
    required=True,
    choices=[
        'HMB-MDS',
        'GRU',
        'HMB',
        'GRU-PUB',
        'GRU-PUB-NPU',
        'MOUSE',
        'HUMAN',
    ],
    help='Set the data use limitation code'
)

parser.add_argument(
    '--skip-post-filters',
    action='store_true',
    default=False,
    help='Skip post-processing filters'
)

args = parser.parse_args()

logging.basicConfig(
    level=args.log_level,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')

AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

MANIFEST_BUCKET = 'anvil-c8f2ca0'

PROJECT_ID = 'igvf-anvil-controlled-access'

PORTAL_UI_URL = 'https://data.igvf.org'

PORTAL_API_URL = 'https://api.data.igvf.org'


@dataclass
class TransferJobProps:
    name: str
    project_id: str
    source_bucket: str
    destination_bucket: str
    manifest_bucket: str
    aws_access_key: str
    aws_secret_access_key: str
    sts_client: StorageTransferServiceClient
    storage_client: StorageClient
    now: datetime
    sleep_time_seconds: int


def generate_name(name: str, source_bucket: str, now: datetime):
    dt = now.strftime("%Y-%m-%d-%H-%M-%S")
    return f'{name}-{source_bucket}-{dt}'


def get_transfer_job(props: TransferJobProps):
    return {
        'name': f'transferJobs/{props.name}',
        'project_id': props.project_id,
        'status': TransferJob.Status.ENABLED,
        'schedule': {
            'schedule_start_date': {
                'year': props.now.year,
                'month': props.now.month,
                'day': props.now.day,
            },
            'schedule_end_date': {
                'year': props.now.year,
                'month': props.now.month,
                'day': props.now.day,
            },
        },
        'transfer_spec': {
            'aws_s3_data_source': {
                'bucket_name': props.source_bucket,
                'aws_access_key': {
                    'access_key_id': props.aws_access_key,
                    'secret_access_key': props.aws_secret_access_key,
                }
            },
            'gcs_data_sink': {
                'bucket_name': props.destination_bucket,
            },
            'transfer_manifest': {
                'location': f'gs://{props.manifest_bucket}/{props.name}.tsv'
            }
        }
    }


def create_transfer_job(props: TransferJobProps):
    transfer_job = get_transfer_job(props)
    response = props.sts_client.create_transfer_job(
        {
            'transfer_job': transfer_job
        }
    )
    logger.info(f'Created job named {props.name}. Response: {response}')


def get_latest_operation(props: TransferJobProps):
    while True:
        job = props.sts_client.get_transfer_job(
            {
                'job_name': f'transferJobs/{props.name}',
                'project_id': props.project_id
            }
        )
        logger.debug(f'Got job: {job}')
        if not job.latest_operation_name:
            print('Waiting for operation')
            time.sleep(5)
        else:
            break
    return props.sts_client.get_operation(
        {
            'name': job.latest_operation_name
        }
    )


def wait_for_transfer_job(props: TransferJob):
    logger.info('Waiting for job to complete')
    while True:
        operation = get_latest_operation(props)
        operation_json = MessageToDict(operation)
        logger.debug(f'Got operation: {operation_json}')
        if operation.done is True:
            if operation_json['metadata']['status'] != 'SUCCESS':
                logger.error(f'Operation failed: {operation_json} {props} {operation}')
                raise ValueError('Error in operation')
            logger.info('Operation completed successfully')
            break
        logger.info(f'Job still running. Status: {operation_json["metadata"]["status"]}')
        time.sleep(props.sleep_time_seconds)


def upload_tsv_to_bucket(tsv: str, props: TransferJob):
    logger.info(f'Uploading STS manifest to gs://{props.manifest_bucket}/{props.name}.tsv')
    bucket = props.storage_client.bucket(props.manifest_bucket)
    blob = bucket.blob(props.name + '.tsv')
    blob.upload_from_string(
        tsv,
        content_type='text/tab-separated-values'
    )


def upload_data_tables(session, data_tables, workspace_namespace, workspace_name, overwrite_tsvs):
    if overwrite_tsvs:
        for name, tsv in data_tables.items():
            logger.info(f'Deleting {name} TSV from {workspace_namespace}/{workspace_name}')
            delete_table_named(
                get_name_from_tsv(tsv),
                workspace_namespace,
                workspace_name,
                session,
            )
        time.sleep(15)
    for name, tsv in data_tables.items():
        logger.info(f'Writing {name} TSV to {workspace_namespace}/{workspace_name}')
        post_tsv_from_memory(
            session,
            workspace_namespace,
            workspace_name,
            tsv,
        )
        time.sleep(1)


def get_config(dul: str, portal_cache: PortalCache, apply_post_filters: bool = True) -> Dict[str, Any]:
    context = {
        'HMB-MDS': {
            'metadata_props': MetadataProps(
                dul='HMB-MDS',
                initial_files_query=(
                    'https://api.data.igvf.org/search/'
                    '?type=File'
                    '&file_set.data_use_limitation_summaries=HMB-MDS'
                    '&status=released'
                    '&frame=object'
                    '&limit=all'
                ),
                portal_cache=portal_cache,
                apply_post_filters=apply_post_filters,
            ),
            'name': 'igvf-anvil-hmb-mds',
            'project_id': PROJECT_ID,
            'manifest_bucket': MANIFEST_BUCKET,
            'destination_bucket': 'fc-secure-915c3459-ce08-44fc-87a8-91986d35519e',
            'sleep_time_seconds': 120,
            'workspace_namespace': 'anvil-datastorage',
            'workspace_name': 'AnVIL_IGVF_HMB_MDS_R1_Staging',
            'overwrite_tsvs': False,
            'preload_searches': [
                (
                    '/search/?type=FileSet'
                    '&data_use_limitation_summaries=HMB-MDS'
                    '&status=released'
                    '&limit=all&frame=object'
                ),
            ]
        },
        'GRU': {
            'metadata_props': MetadataProps(
                dul='GRU',
                initial_files_query=(
                    'https://api.data.igvf.org/search/'
                    '?type=File'
                    '&file_set.data_use_limitation_summaries=GRU'
                    '&status=released'
                    '&frame=object'
                    '&limit=all'
                ),
                portal_cache=portal_cache,
                apply_post_filters=apply_post_filters,
            ),
            'name': 'igvf-anvil-gru',
            'project_id': PROJECT_ID,
            'manifest_bucket': MANIFEST_BUCKET,
            'destination_bucket': 'fc-secure-3123e3cc-6d9c-4867-8595-45558d34727a',
            'sleep_time_seconds': 120,
            'workspace_namespace': 'anvil-datastorage',
            'workspace_name': 'AnVIL_IGVF_GRU_R1_Staging',
            'overwrite_tsvs': False,
            'preload_searches': [
                (
                    '/search/?type=FileSet'
                    '&data_use_limitation_summaries=GRU'
                    '&status=released'
                    '&limit=all&frame=object'
                )
            ]
        },
        'HMB': {
            'metadata_props': MetadataProps(
                dul='HMB',
                initial_files_query=(
                    'https://api.data.igvf.org/search/'
                    '?type=File'
                    '&file_set.data_use_limitation_summaries=HMB'
                    '&status=released'
                    '&frame=object'
                    '&limit=all'
                ),
                portal_cache=portal_cache,
                apply_post_filters=apply_post_filters,
            ),
            'name': 'igvf-anvil-hmb',
            'project_id': PROJECT_ID,
            'manifest_bucket': MANIFEST_BUCKET,
            'destination_bucket': 'fc-secure-c8192a91-0c4b-4468-92eb-56394cca1331',
            'sleep_time_seconds': 120,
            'workspace_namespace': 'anvil-datastorage',
            'workspace_name': 'AnVIL_IGVF_HMB_R1_Staging',
            'overwrite_tsvs': False,
            'preload_searches': [
                 (
                    '/search/?type=FileSet'
                    '&data_use_limitation_summaries=HMB'
                    '&status=released'
                    '&limit=all&frame=object'
                )
            ]
        },
        'GRU-PUB': {
            'metadata_props': MetadataProps(
                dul='GRU-PUB',
                initial_files_query=(
                    'https://api.data.igvf.org/search/'
                    '?type=File'
                    '&file_set.data_use_limitation_summaries=GRU-PUB'
                    '&status=released'
                    '&frame=object'
                    '&limit=all'
                ),
                portal_cache=portal_cache,
                apply_post_filters=apply_post_filters,
            ),
            'name': 'igvf-anvil-gru-pub',
            'project_id': PROJECT_ID,
            'manifest_bucket': MANIFEST_BUCKET,
            'destination_bucket': 'fc-secure-76bef3ab-c256-49d6-ae3d-b8c9c4834098',
            'sleep_time_seconds': 120,
            'workspace_namespace': 'anvil-datastorage',
            'workspace_name': 'AnVIL_IGVF_GRU_PUB_R1_Staging',
            'overwrite_tsvs': False,
            'preload_searches': [
                (
                    '/search/?type=FileSet'
                    '&data_use_limitation_summaries=GRU-PUB'
                    '&status=released'
                    '&limit=all&frame=object'
                )
            ]
        },
        'GRU-PUB-NPU': {
            'metadata_props': MetadataProps(
                dul='GRU-PUB-NPU',
                initial_files_query=(
                    'https://api.data.igvf.org/search/'
                    '?type=File'
                    '&file_set.data_use_limitation_summaries=GRU-PUB-NPU'
                    '&status=released'
                    '&frame=object'
                    '&limit=all'
                ),
                portal_cache=portal_cache,
                apply_post_filters=apply_post_filters,
            ),
            'name': 'igvf-anvil-gru-pub-npu',
            'project_id': PROJECT_ID,
            'manifest_bucket': MANIFEST_BUCKET,
            'destination_bucket': 'todoo#####',
            'sleep_time_seconds': 120,
            'workspace_namespace': 'anvil-datastorage',
            'workspace_name': 'todo####',
            'overwrite_tsvs': False,
            'preload_searches': [
                (
                    '/search/?type=FileSet'
                    '&data_use_limitation_summaries=GRU-PUB-NPU'
                    '&status=released'
                    '&limit=all&frame=object'
                )
            ]
        },
        'MOUSE': {
            'metadata_props': MetadataProps(
                dul='MOUSE',
                initial_files_query=(
                    'https://api.data.igvf.org/search/'
                    '?type=File'
                    '&lab.title!=Charles+Gersbach%2C+Duke'
                    '&file_set.samples.taxa=Mus+musculus'
                    '&status=released'
                    '&frame=object'
                    '&limit=all'
                ),
                portal_cache=portal_cache,
                apply_post_filters=apply_post_filters,
            ),
            'name': 'igvf-mouse',
            'project_id': PROJECT_ID,
            'manifest_bucket': MANIFEST_BUCKET,
            'destination_bucket': 'todo####',
            'sleep_time_seconds': 120,
            'workspace_namespace': 'anvil-datastorage',
            'workspace_name': 'Todoo###',
            'overwrite_tsvs': False,
            'preload_searches': [
                (
                    '/search/?type=FileSet'
                    '&donors.taxa=Mus+musculus'
                    '&lab.title!=Charles+Gersbach%2C+Duke'
                    '&status=released'
                    '&limit=all&frame=object'
                )
            ]
        },
        'HUMAN': {
            'metadata_props': MetadataProps(
                dul='HUMAN',
                initial_files_query=(
                    'https://api.data.igvf.org/search/'
                    '?type=File'
                    '&file_set.samples.taxa=Homo+sapiens'
                    '&file_set.data_use_limitation_summaries=No+limitations'
                    '&file_set.data_use_limitation_summaries=no+certificate'
                    '&lab.title=Lea+Starita%2C+UW'
                    '&lab.title=Doug+Fowler%2C+UW'
                    '&lab.title=Thomas+Quertermous%2C+Stanford'
                    '&lab.title=Karen+Mohlke%2C+UNC'
                    '&lab.title=Ryan+Corces%2C+Gladstone+Institute+UCSF'
                    '&lab.title=Ansuman+Satpathy%2C+Stanford'
                    '&lab.title=Jason+Buenrostro%2C+Broad'
                    '&lab.title=Jay+Shendure%2C+UW'
                    '&status=released'
                    '&frame=object'
                    '&limit=all'
                ),
                portal_cache=portal_cache,
                apply_post_filters=apply_post_filters,
            ),
            'name': 'igvf-human',
            'project_id': PROJECT_ID,
            'manifest_bucket': MANIFEST_BUCKET,
            'destination_bucket': 'todo####',
            'sleep_time_seconds': 120,
            'workspace_namespace': 'anvil-datastorage',
            'workspace_name': 'Todoo###',
            'overwrite_tsvs': False,
            'preload_searches': [
                (
                    '/search/?type=FileSet'
                    '&donors.taxa=Homo+sapiens'
                    '&data_use_limitation_summaries=No+limitations'
                    '&data_use_limitation_summaries=no+certificate'
                    '&status=released'
                    '&limit=all&frame=object'
                )
            ]
        }
    }
    return context[dul]


def main():
    session = get_session()
    sts_client = StorageTransferServiceClient()
    storage_client = StorageClient()
    portal_cache = PortalCache(
        props=PortalCacheProps(
            url=PORTAL_API_URL,
            async_portal_api=AsyncIgvfApi,
        )
    )
    config = get_config(
        args.dul,
        portal_cache,
        apply_post_filters=not args.skip_post_filters,
    )
    metadata_props = config['metadata_props']
    metadata_props.portal_cache.preload(
        config['preload_searches']
    )
    metadata = asyncio.run(
        collect_metadata(
            metadata_props
        )
    )
    manifests = make_sts_manifests_from_metadata(
        metadata,
        metadata_props
    )
    now = datetime.now()
    transfer_job_props = []
    for source_bucket, tsv in manifests.items():
        props = TransferJobProps(
            name=generate_name(
                config['name'],
                source_bucket,
                now
            ),
            project_id=config['project_id'],
            source_bucket=source_bucket,
            destination_bucket=config['destination_bucket'],
            manifest_bucket=config['manifest_bucket'],
            aws_access_key=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            sts_client=sts_client,
            storage_client=storage_client,
            now=now,
            sleep_time_seconds=config['sleep_time_seconds'],
        )
        transfer_job_props.append(
            props
        )
        upload_tsv_to_bucket(
            tsv,
            props
        )
        create_transfer_job(props)
    for source_bucket, tsv in manifests.items():
        wait_for_transfer_job(props)
    data_tables = asyncio.run(
        make_data_tables(
            metadata,
            metadata_props,
            config['destination_bucket'],
            PORTAL_UI_URL,
        )
    )
    for table_name, tsv in data_tables.items():
        with open(
                f'igvf_anvil_{args.dul.lower().replace("-", "_")}_{table_name}.tsv',
                'w'
        ) as f:
            f.write(tsv)
    upload_data_tables(
        session,
        data_tables,
        config['workspace_namespace'],
        config['workspace_name'],
        config['overwrite_tsvs']
    )


if __name__ == "__main__":
    main()
