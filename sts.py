import os

import time

from typing import Dict, List, Set

import requests

from google.cloud.storage_transfer import StorageTransferServiceClient
from google.cloud.storage_transfer import TransferJob

from google.protobuf.json_format import MessageToDict

from datetime import datetime

from google.oauth2 import service_account

from dataclasses import dataclass

import logging

import argparse


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

args = parser.parse_args()

logging.basicConfig(
    level=args.log_level,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']

AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

MANIFEST_LOCATION = 'gs://test-pulumi-bucket-58b2c6f/fm1.tsv'

MANIFEST_BUCKET = 'test-pulumi-bucket-58b2c6f'


@dataclass
class TransferJobProps:
    name: str
    project_id: str
    source_bucket: str
    destination_bucket: str
    manifest_bucket: str
    aws_access_key: str
    aws_secret_access_key: str
    client: StorageTransferServiceClient
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
                'location': f'gs://{props.manifest_bucket}/{props.name}'
            }
        }
    }


def create_transfer_job(props: TransferJobProps):
    transfer_job = get_transfer_job(props)
    response = props.client.create_transfer_job(
        {
            'transfer_job': transfer_job
        }
    )
    logger.info(f'Created job named {props.name}. Response: {response}')


def get_latest_operation(props: TransferJobProps):
    while True:
        job = props.client.get_transfer_job(
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
    return props.client.get_operation(
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


def copy_sts_manifest_to_bucket(tsv: str, props: TransferJob):
    pass


if __name__ == '__main__':
    client = StorageTransferServiceClient()
    now = datetime.now()
    # Generate metadata
    # Generate files_to_move tsvs, grouped by source bucket
    # Put file tsv manifests in Google bucket (named by job name, source_bucket, date)
    # For each source bucket, launchs jobs
    # For each each job, monitor until all successful
    # Upload metadata tables
    source_bucket = 'hic-files-transfer'
    props = TransferJobProps(
        name=generate_name(
            'test-sts-py',
            source_bucket,
            now
        ),
        project_id='encode-dcc-1016',
        source_bucket=source_bucket,
        destination_bucket='test-pulumi-bucket-58b2c6f',
        manifest_bucket=MANIFEST_BUCKET,
        aws_access_key=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        client=client,
        now=now,
        sleep_time_seconds=120,
    )
    create_transfer_job(props)
    wait_for_transfer_job(props)
