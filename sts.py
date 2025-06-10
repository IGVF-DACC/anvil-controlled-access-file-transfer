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


AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']

AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

MANIFEST_LOCATION = 'gs://test-pulumi-bucket-58b2c6f/fm1.tsv'


@dataclass
class TransferJobProps:
    name: str
    project_id: str
    source_bucket: str
    destination_bucket: str
    manifest_location: str
    aws_access_key: str
    aws_secret_access_key: str
    client: StorageTransferServiceClient
    now: datetime


def generate_job_name(name: str, now: datetime):
    dt = now.strftime("%Y-%m-%d-%H-%M-%S")
    return f'transferJobs/{name}-{dt}'


def get_transfer_job(props: TransferJobProps):
    return {
        'name': props.name,
        'project_id': props.project_id,
        'status': TransferJob.Status.ENABLED,
        'schedule': {
            'schedule_start_date': {
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
                'location': props.manifest_location,
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
    print('Created job named', props.name, response)


def get_latest_operation(props: TransferJobProps):
    while True:
        job = props.client.get_transfer_job(
            {
                'job_name': props.name,
                'project_id': props.project_id
            }
        )
        print('got job', job)
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
    print('Waiting for job')
    while True:
        operation = get_latest_operation(props)
        operation_json = MessageToDict(operation)
        print('Got operation', operation, operation_json)
        if operation.done is True:
            print('Operation is done', operation, operation_json, props)
            if operation_json['metadata']['status'] != 'SUCCESS':
                raise ValueError('Error in operation')
            break
        print('Job still running, waiting')
        print(operation_json['metadata']['status'])
        time.sleep(60)


if __name__ == '__main__':
    client = StorageTransferServiceClient()
    now = datetime.now()
    props = TransferJobProps(
        name=generate_job_name(
            'test-sts-py',
            now
        ),
        project_id='encode-dcc-1016',
        source_bucket='hic-files-transfer',
        destination_bucket='test-pulumi-bucket-58b2c6f',
        manifest_location=MANIFEST_LOCATION,
        aws_access_key=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        client=client,
        now=now,
    )
    create_transfer_job(props)
    wait_for_transfer_job(props)
