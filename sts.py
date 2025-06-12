import os

import time

from typing import Dict, List, Set

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

from metadata import collect_metadata
from metadata import MetadataProps
from metadata import make_sts_manifests_from_metadata
from metadata import make_data_tables

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
    ],
    help='Set the data use limitation code'
)

args = parser.parse_args()

logging.basicConfig(
    level=args.log_level,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')

AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

MANIFEST_BUCKET = 'test-pulumi-bucket-58b2c6f'

PROJECT_ID = 'encode-dcc-1016'


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


context = {
    'HMB-MDS': {
        'metadata_props': MetadataProps(
            portal_url='https://api.data.igvf.org',
            dul='HMB-MDS',
            initial_files_query=(
                'https://api.data.igvf.org/search/'
                '?type=File'
                '&file_set.data_use_limitation_summaries=HMB-MDS'
                '&file_set.controlled_access=true'
                '&status=released'
                '&frame=object'
                '&limit=all'
            )
        ),
        'name': 'igvf-anvil-hmb-mds',
        'project_id': PROJECT_ID,
        'manifest_bucket': MANIFEST_BUCKET,
        'destination_bucket': 'test-pulumi-bucket-58b2c6f',
        'workspace_namespace': 'DACC_ANVIL',
        'workspace_name': 'IGVF AnVIL Sandbox',
        'sleep_time_seconds': 120,
    }
}


if __name__ == '__main__':
    sts_client = StorageTransferServiceClient()
    storage_client = StorageClient()
    config = context[args.dul]
    metadata_props = config['metadata_props']
    metadata = collect_metadata(
        metadata_props
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
        #upload_tsv_to_bucket(
        #    tsv,
        #    props
        #)
        # create_transfer_job(props)
        # wait_for_transfer_job(props)
    data_tables = make_data_tables(
        metadata,
        config['destination_bucket'],
    )
    print(manifests)
    '''
    # Generate metadata
    # Generate files_to_move tsvs, grouped by source bucket
    # Put file tsv manifests in Google bucket (named by job name, source_bucket, date)
    # For each source bucket, launchs jobs
    # For each each job, monitor until all successful
    # Upload metadata tables
    tsv = 'ENCFF053BBK.fastq.gz\nENCFF110XAL.fastq.gz'
    create_transfer_job(props)
    wait_for_transfer_job(props)
    '''
