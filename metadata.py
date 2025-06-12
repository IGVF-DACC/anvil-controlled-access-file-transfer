import json

import requests

from dataclasses import dataclass

from typing import Callable, Any, Dict

from google.auth import compute_engine
from google.auth.transport.requests import AuthorizedSession

import logging

logger = logging.getLogger(__name__)


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
    's3_uri',
]


FILESET_FIELDS = [
    '@id',
    '@type',
    'accession',
    'assay_term',
    'assay_titles',
    'associated_phenotypes',
    'auxiliary_sets',
    'average_guide_coverage',
    'average_insert_size',
    'award',
    'barcode_map',
    'construct_library_sets',
    'control_file_sets',
    'exon',
    'file_set_type',
    'guide_type',
    'input_file_sets',
    'lab',
    'preferred_assay_title',
    'sample_summary',
    'samples',
    'scope',
    'selection_criteria',
    'sequencing_library_types',
    'small_scale_gene_list',
    'small_scale_loci_list',
    'summary',
    'targeted_genes',
]


SAMPLE_FIElDS = [
    '@id',
    'accession'
    'age_units',
    'biosample_qualifiers',
    'classifications',
    'construct_library_sets',
    'donors',
    'embryonic',
    'lower_bound_age',
    'modifications',
    'moi',
    'multiplexed_samples',
    'pooled_from',
    'sample_terms',
    'sorted_fractions',
    'summary',
    'targeted_sample_term',
    'upper_bound_age',
]


DONOR_FIELDS = [
    '@id',
    'accession',
    'ethnicities',
    'phenotypic_features',
    'sex',
    'taxa',
]


@dataclass
class MetadataProps:
    portal_url: str
    dul: str
    initial_files_query: str
    workspace_namespace: str
    workspace_name: str


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


def post_tsv_from_memory(session, workspace_namespace, workspace_name, in_memory_tsv, overwrite=False):
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


def print_summary(files_seen, file_sets_seen, samples_seen, donors_seen, full=False):
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
    if full:
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


def collect_metadata(props: MetadataProps) -> Dict[str, Any]:
    files_seen = set()
    files_local = {}
    samples_seen = set()
    samples_local = {}
    donors_seen = set()
    donors_local = {}
    file_sets_seen = set()
    file_sets_local = {}
    files = requests.get(files_url).json()['@graph']
    for f in files:
        files_local[f['@id']] = f
        files_seen.add(f['@id'])
    file_sets = {
        f['file_set']
        for f in files
    }
    print(
        file_sets,
        len(file_sets),
        len(files)
    )
    while file_sets:
        fs = file_sets.pop()
        if fs in file_sets_seen:
            continue
        file_sets_seen.add(fs)
        full_fs = requests.get(props.portal_url + fs + '@@object').json()
        file_sets_local[fs] = full_fs
        if 'input_file_sets' in full_fs:
            for ifs in full_fs['input_file_sets']:
                print('Found input file set')
                if ifs not in file_sets_seen:
                    file_sets.add(ifs)
        if 'files' in full_fs:
            print('Getting files')
            for f in full_fs['files']:
                if f not in files_seen:
                    full_file = requests.get(props.portal_url + f + '@@object').json()
                    files_local[f] = full_file
                    files_seen.add(f)
        if 'samples' in full_fs:
            print('Getting samples')
            samples = requests.get(
                props.portal_url + f'/search/?type=Sample&file_sets.@id={fs}&frame=object&limit=all'
            ).json()['@graph']
            for sample in samples:
                if sample['@id'] not in samples_seen:
                    samples_seen.add(sample['@id'])
                    samples_local[sample['@id']] = sample
        if 'donors' in full_fs:
            print('Getting donors')
            for donor in full_fs['donors']:
                if donor not in donors_seen:
                    donors_seen.add(donor)
                    full_donor = requests.get(
                        props.portal_url + donor + '@@object'
                    ).json()
                    donors_local[donor] = full_donor
    print_summary(
        files_seen,
        file_sets_seen,
        samples_seen,
        donors_seen,
    )
    metadata = {
        'seen': {
            'files': list(sorted(files_seen)),
            'file_sets': list(sorted(file_sets_seen)),
            'samples': list(sorted(samples_seen)),
            'donors': list(sorted(donors_seen)),
        },
        'local': {
            'files': files_local,
            'file_sets': file_sets_local,
            'samples': samples_local,
            'donors': donors_local,
        }
    }
    return metadata


HMB_MDS_METDATA_PROPS = MetadataProps(
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
    ),
    workspace_namespace='DACC_ANVIL',
    workspace_name='IGVF AnVIL Sandbox'
)
