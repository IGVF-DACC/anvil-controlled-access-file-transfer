import os

import time

from typing import Dict, List, Set

import requests

from google.cloud.storage_transfer import StorageTransferServiceClient
from google.cloud.storage_transfer import TransferJob

from datetime import date

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


def get_transfer_job(props: TransferJobProps):
    now = date.today()
    return {
        'name': f'transferJobs/{props.name}',
        'project_id': props.project_id,
        'status': TransferJob.Status.ENABLED,
        'schedule': {
            'schedule_start_date': {
                'year': now.year,
                'month': now.month,
                'day': now.day,
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
    print('Created job', response)


def wait_for_transfer_job(props: TransferJob):
    while True:
        job = props.client.get_transfer_job(
            {
                'job_name': f'transferJobs/{props.name}',
                'project_id': props.project_id
            }
        )
        print('Waiting for job')
        print('Got job', job)
        if job.status == TransferJob.Status.SUCCESS:
            print('Transfer job completed successfully', props)
            break
        elif job.status == TransferJob.Status.FAILED:
            print('Transfer job failed')
            raise ValueError('Transfer job failed', props)
        print('Job still running, waiting')
        time.sleep(30)



if __name__ == '__main__':
    client = StorageTransferServiceClient()
    props = TransferJobProps(
        name='test-sts-py-1',
        project_id='encode-dcc-1016',
        source_bucket='hic-files-transfer',
        destination_bucket='test-pulumi-bucket-58b2c6f',
        manifest_location=MANIFEST_LOCATION,
        aws_access_key=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        client=client,
    )
    create_transfer_job(props)
    wait_for_transfer_job(props)
