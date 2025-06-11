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
