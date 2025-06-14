import json

import csv

import requests

import logging

import time

from io import StringIO

from dataclasses import dataclass

from typing import Callable, Any, Dict, Tuple

from google.auth import compute_engine
from google.auth.transport.requests import AuthorizedSession


logger = logging.getLogger(__name__)


AT_ID_LINKS = [
    'seqspecs',
    'derived_from',
    'file_set',
    'files',
    'input_file_sets',
    'samples',
    'donors',
    'donor_id',
    'biosample_id',
    'file_id',
    'file_set_id',
]

# sequencing_platform.platform_term
# lab -> lab.title
#' Phenotypic_feature.feature.term_name'

FILE_FIELDS = [
    'type',
    'summary',
    'assay_titles',
    'content_type',
    'file_name',
    'file_format',
    'file_format_type',
    'file_size',
    'file_md5sum',
    'file_set',
    'seqspecs',
    'workflow',
    'derived_from',
    'reference_assembly', # 'assembly'
    'cell_type_annotation',
    'flowcell_id',
    'illumina_read_type',
    'lane',
    'mean_read_length',
    'sequencing_kit',
    'sequencing_platform',
    'sequencing_run',
    'transcriptome_annotation',
]


FILE_SET_FIELDS = [
    'type',
    'assay_term',
    'assay_titles',
    'files',
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
    'biosample_id', # 'samples'
    'scope',
    'selection_criteria',
    'sequencing_library_types',
    'small_scale_gene_list',
    'small_scale_loci_list',
    'summary',
    'targeted_genes',
]


SAMPLE_FIELDS = [
    'type',
    'summary',
    'sample_terms',
    'modifications',
    'targeted_sample_term',
    'biosample_type', # 'classifications'
    'multiplexed_samples',
    'pooled_from',
    'donor_id', #'donors',
    'construct_library_sets',
    'biosample_qualifiers',
    'embryonic',
    'sorted_fractions',
    'donor_age_at_collection_unit_upper_bound', # 'upper_bound_age'
    'donor_age_at_collection_unit_lower_bound', # 'lower_bound_age'
    'donor_age_at_collection_unit', # 'age_units'
    'moi',
]


DONOR_FIELDS = [
    'type',
    'reported_ethnicity', # 'ethnicities'
    'phenotypic_features',
    'phenotypic_sex', # 'sex'
    'organism_type', # 'taxa'
]


@dataclass
class MetadataProps:
    portal_url: str
    dul: str
    initial_files_query: str


def get_session():
    # For VM with attached SA.
    credentials = compute_engine.Credentials()
    return AuthorizedSession(credentials)


def get_name_from_tsv(tsv):
    first_header = tsv.split('\t')[0]
    if '_id' in first_header:
        return first_header.replace('_id', '')
    return first_header


def delete_table_named(name, workspace_namespace, workspace_name, session):
    delete_url = f'https://api.firecloud.org/api/workspaces/{workspace_namespace}/{workspace_name}/entityTypes/{name}'
    response = session.delete(delete_url)
    logger.info(f'Delete response {response.status_code} {response.text}')


def post_tsv_from_memory(session, workspace_namespace, workspace_name, in_memory_tsv):
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
    logger.info(f'Post response {response.status_code} {response.text}')
    if response.status_code != 200:
        raise ValueError('Post response not 200, not continuing')


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
    files = requests.get(
        props.initial_files_query
    ).json()['@graph']
    for f in files:
        files_local[f['@id']] = f
        files_seen.add(f['@id'])
    file_sets = {
        f['file_set']
        for f in files
    }
    logger.info(f'Found {len(files)} files and {len(file_sets)} file_sets')
    while file_sets:
        fs = file_sets.pop()
        if fs in file_sets_seen:
            continue
        file_sets_seen.add(fs)
        full_fs = requests.get(props.portal_url + fs + '@@object').json()
        file_sets_local[fs] = full_fs
        if 'input_file_sets' in full_fs:
            print('Getting file_sets')
            for ifs in full_fs['input_file_sets']:
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


def parse_s3_uri_into_bucket_and_path(s3_uri: str) -> Tuple[str, str]:
    return tuple(s3_uri.split('s3://')[1].split('/', 1))


def make_sts_manifests_from_metadata(metadata: Dict[str, Any], props: MetadataProps) -> Dict[str, Any]:
    s3_uris = list(
        sorted(
            {
                parse_s3_uri_into_bucket_and_path(
                    metadata['local']['files'][f]['s3_uri']
                )
                for f in metadata['seen']['files']
            }
        )
    )
    grouped_by_bucket = {}
    for bucket, path in s3_uris:
        if bucket not in grouped_by_bucket:
            grouped_by_bucket[bucket] = []
        grouped_by_bucket[bucket].append(path)
    for bucket, paths in grouped_by_bucket.items():
        print(f'Bucket {bucket} has {len(paths)} files')
    return {
        k: '\n'.join(v)
        for k, v in grouped_by_bucket.items()
    }


def make_gs_file_path_from_s3_uri(destination_bucket: str, s3_uri: str) -> str:
    _, path = parse_s3_uri_into_bucket_and_path(s3_uri)
    return f'gs://{destination_bucket}/{path}'


def serialize_cell(value):
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    if isinstance(value, (int, float)):
        return str(value)
    return value


def parse_accession_from_at_ids(at_ids) -> list[str]:
    return [
        at_id.split('/')[2]
        for at_id in at_ids
    ]


def add_fields_to_row(item, fields, row, name):
    for field in fields:
        value = None
        if field == 'type':
            value = item['@type'][0]
        elif field == 'id':
            value = item['@id']
        elif field == 'file_name':
            value = item['s3_uri'].split('/')[-1]
        elif field == 'file_md5sum':
            value = item['md5sum']
        elif name == 'files' and field == 'reference_assembly':
            value = item.get('assembly', '')
        elif name == 'file_sets' and field == 'biosample_id':
            value = item.get('samples', '')
        elif name == 'donors' and field == 'organism_type':
            value = item.get('taxa', '')
        elif name == 'donors' and field == 'phenotypic_sex':
            value = item.get('sex', '')
        elif name == 'donors' and field == 'reported_ethnicity':
            value = item.get('ethnicities', '')
        elif name == 'samples' and field == 'biosample_type':
            value = item.get('classifications', '')
        elif name == 'samples' and field == 'donor_id':
            value = item.get('donors', '')
        elif name == 'samples' and field == 'donor_age_at_collection_unit_upper_bound':
            value = item.get('upper_bound_age', '')
        elif name == 'samples' and field == 'donor_age_at_collection_unit_lower_bound':
            value = item.get('lower_bound_age', '')
        elif name == 'samples' and field == 'donor_age_at_collection_unit':
            value = item.get('age_units', '')
        else:
            value = item.get(field, '')
        if field in AT_ID_LINKS:
            value = value or item.get(field, '')
            if not value:
                value = value
            elif isinstance(value, str):
                value = parse_accession_from_at_ids(
                    [
                        value
                    ]
                )[0]
            else:
                value = parse_accession_from_at_ids(
                    value
                )
        assert value is not None
        row.append(
            serialize_cell(
                value
            )
        )


def make_data_tables(metadata: Dict[str, Any], destination_bucket: str, portal_url: str) -> Dict[str, Any]:
    file_headers = ['file_id', 'file_path', 'igvf_portal_url'] + FILE_FIELDS
    files_tsv = '\t'.join(file_headers)
    for f in metadata['seen']['files']:
        full_file = metadata['local']['files'][f]
        row = [
            full_file['accession'],
            make_gs_file_path_from_s3_uri(
                destination_bucket,
                full_file['s3_uri']
            ),
            portal_url + full_file['@id'],
        ]
        add_fields_to_row(full_file, FILE_FIELDS, row, 'files')
        files_tsv = files_tsv + '\n' + '\t'.join(row)

    file_set_headers = ['file_set_id', 'igvf_portal_url'] + FILE_SET_FIELDS
    file_sets_tsv = '\t'.join(file_set_headers)
    for fs in metadata['seen']['file_sets']:
        full_fs = metadata['local']['file_sets'][fs]
        row = [
            full_fs['accession'],
            portal_url + full_fs['@id'],
        ]
        add_fields_to_row(full_fs, FILE_SET_FIELDS, row, 'file_sets')
        file_sets_tsv = file_sets_tsv + '\n' + '\t'.join(row)

    sample_headers = ['sample_id', 'igvf_portal_url'] + SAMPLE_FIELDS
    samples_tsv = '\t'.join(sample_headers)
    for s in metadata['seen']['samples']:
        full_s = metadata['local']['samples'][s]
        row = [
            full_s['accession'],
            portal_url + full_s['@id'],
        ]
        add_fields_to_row(full_s, SAMPLE_FIELDS, row, 'samples')
        samples_tsv = samples_tsv + '\n' + '\t'.join(row)

    donor_headers = ['donor_id', 'igvf_portal_url'] + DONOR_FIELDS
    donors_tsv = '\t'.join(donor_headers)
    for d in metadata['seen']['donors']:
        full_d = metadata['local']['donors'][d]
        row = [
            full_d['accession'],
            portal_url + full_d['@id'],
        ]
        add_fields_to_row(full_d, DONOR_FIELDS, row, 'donors')
        donors_tsv = donors_tsv + '\n' + '\t'.join(row)
    return {
        'files': files_tsv,
        'file_sets': file_sets_tsv,
        'samples': samples_tsv,
        'donors': donors_tsv,
    }
