import json

import requests

from dataclasses import dataclass

from typing import Callable, Any

from google.auth import compute_engine
from google.auth.transport.requests import AuthorizedSession


FILE_FIELDS = [
    '@id',
    '@type',
    'accession',
    'assay_titles',
    'assembly',
    'base_modifications',
    'cell_type_annotation',
    'content_summary',
    'content_type',
    'derived_from',
    'file_format',
    'file_format_type',
    'file_set',
    'file_size',
    'flowcell_id',
    'illumina_read_type',
    'lane',
    'md5sum',
    'mean_read_length',
    'seqspecs',
    'sequencing_kit',
    'sequencing_platform',
    'sequencing_run',
    'seqspec_of',
    'summary',
    'transcriptome_annotation',
    'workflow',
]


HMB_MDS_FILES_QUERY = ''


@dataclass
class MetadataProps:
    portal_url: str
    dul: str
    files_query: str
    crawler: Callable[..., Any]


def get_session():
    # For VM with attached SA.
    credentials = compute_engine.Credentials()
    return AuthorizedSession(credentials)


def get_name_from_tsv(tsv):
    first_header = tsv.split('\t')[0]
    if '_' in first_header:
        return first_header.split('_')[0]
    return first_header


def delete_table_named(name, workspace_namespace, workspace_name, session):
    delete_url = f'https://api.firecloud.org/api/workspaces/{workspace_namespace}/{workspace_name}/entityTypes/{name}'
    response = session.delete(delete_url)
    print(response.text)


def post_tsv_from_memory(session, workspace_namespace, workspace_name, in_memory_tsv, overwrite=True):
    if overwrite:
        name = get_name_from_tsv(in_memory_tsv)
        delete_table_named(name, workspace_namespace, workspace_name, session)
    url = f'https://api.firecloud.org/api/workspaces/{workspace_namespace}/{workspace_name}/flexibleImportEntities'
    response = session.post(
        url,
        files={
            'entities': 
            (
                'entities.tsv', 
                in_memory_tsv, 
                'text/tab-separated-values'
            ),
        }
    )
    print(response.text)


workspace_namespace = 'DACC_ANVIL'
workspace_name = 'IGVF AnVIL Sandbox'


base_url = 'https://api.data.igvf.org'

files_url = (
    'https://api.data.igvf.org/search/'
    '?type=File&file_set.data_use_limitation_summaries=HMB-MDS'
    '&file_set.controlled_access=true'
    '&field=file_set.data_use_limitation_summaries'
    '&field=file_set.controlled_access'
    '&field=file_set.@id'
    '&limit=all'
)

files = requests.get(files_url).json()['@graph']


files_seen = set()
files_local = {}

samples_seen = set()
samples_local = {}

donors_seen = set()
donors_local = {}

file_sets_seen = set()
file_sets_local = {}

for f in files:
    files_local[f['@id']] = f
    files_seen.add(f['@id'])

file_sets = {f['file_set']['@id'] for f in files}

print(file_sets, len(file_sets), len(files))


while file_sets:
    fs = file_sets.pop()
    if fs in file_sets_seen:
        continue
    file_sets_seen.add(fs)
    full_fs = requests.get(base_url + fs + '@@object').json()
    file_sets_local[fs] = full_fs
    if 'input_file_sets' in full_fs:
        for ifs in full_fs['input_file_sets']:
            print('Found input file set')
            if ifs not in file_sets_seen:
                file_sets.add(ifs)
    if 'files' in full_fs:
        for f in full_fs['files']:
            if f not in files_seen:
                full_file = requests.get(base_url + f + '@@object').json()
                files_local[f] = full_file
                files_seen.add(f)
    if 'samples' in full_fs:
        print('GETTING SAMPLES')
        samples = requests.get(base_url + f'/search/?type=Sample&file_sets.@id={fs}&frame=object&limit=all').json()['@graph']
        for sample in samples:
            samples_seen.add(sample['@id'])
            samples_local[sample['@id']] = sample
    if 'donors' in full_fs:
        print('Getting donors')
        for donor in full_fs['donors']:
            if donor not in donors_seen:
                donors_seen.add(donor)
                print('getting donor', donor)
                full_donor = requests.get(base_url + donor + '@@object').json()
                donors_local[donor] = full_donor


def print_summary(files_seen, file_sets_seen, samples_seen, donors_seen):
    print(
        json.dumps(
            {
                'files': len(list(sorted(files_seen))),
                'file_sets': len(list(sorted(file_sets_seen))),
                'samples': len(list(sorted(samples_seen))),
                'donors': len(list(sorted(donors_seen))),
            },
            indent=4
        )
    )
    print(
        json.dumps(
            {
                'files': list(sorted(files_seen)),
                'file_sets': list(sorted(file_sets_seen)),
                'samples': list(sorted(samples_seen)),
                'donors': list(sorted(donors_seen)),
            },
            indent=4
        )
    )


print_summary(files_seen, file_sets_seen, samples_seen, donors_seen)
