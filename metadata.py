import json

import csv

import logging

import time

from io import StringIO

from dataclasses import dataclass

from typing import Callable, Any, Dict, Tuple, List

from google.auth import compute_engine
from google.auth.transport.requests import AuthorizedSession

from cache import PortalCache

import aiohttp

from igvf_async_client import AsyncIgvfApi


logger = logging.getLogger(__name__)


AT_ID_LINKS = [
    'seqspecs',
    'derived_from',
    'file_set',
    'files',
    'input_file_sets',
    'samples',
    'donors',
    'file_id',
    'file_set_id',
]


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
    'workflows',
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
    'preferred_assay_titles',
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


SAMPLE_FIELDS = [
    'type',
    'summary',
    'sample_terms',
    'modifications',
    'targeted_sample_term',
    'biosample_type', # 'classifications'
    'multiplexed_samples',
    'pooled_from',
    'donors',
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
    dul: str
    initial_files_query: str
    portal_cache: PortalCache


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


# Some AsyncIgvfApi client interals don't like running across
# independent asyncio.run() calls.
async def reset_async_portal_api(api: AsyncIgvfApi):
    await api.api_client.close()
    api.api_client.set_default(None) # Doesn't create new session without setting this back to None.


async def async_get_json(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()


async def collect_metadata(props: MetadataProps) -> Dict[str, Any]:
    async_portal_api = props.portal_cache.props.async_portal_api()
    files_seen = set()
    samples_seen = set()
    donors_seen = set()
    file_sets_seen = set()
    files = (
        await async_get_json(
            props.initial_files_query
        )
    )['@graph']
    for f in files:
        props.portal_cache.local[f['@id']] = f
        files_seen.add(f['@id'])
    file_sets = {
        f['file_set']
        for f in files
    }
    logger.info(f'Found {len(files)} files and {len(file_sets)} file_sets')
    logger.info('Loading filesets into cache')
    await props.portal_cache.async_batch_get(
        file_sets,
        async_portal_api,
    )
    while file_sets:
        fs = file_sets.pop()
        if fs in file_sets_seen:
            continue
        file_sets_seen.add(fs)
        full_fs = (
            await props.portal_cache.async_batch_get(
                [fs],
                async_portal_api,
            )
        )[fs]
        if 'input_file_sets' in full_fs:
            print('Getting file_sets')
            for ifs in full_fs['input_file_sets']:
                if ifs not in file_sets_seen:
                    file_sets.add(ifs)
        if 'files' in full_fs:
            print('Getting files')
            await props.portal_cache.async_batch_get(
                full_fs['files'],
                async_portal_api,
            )
            for f in full_fs['files']:
                if f not in files_seen:
                    files_seen.add(f)
        if 'samples' in full_fs:
            print('Getting samples')
            samples = (
                await async_get_json(
                    props.portal_cache.props.url + f'/search/?type=Sample&file_sets.@id={fs}&frame=object&limit=all'
                )
            )['@graph']
            for sample in samples:
                props.portal_cache.local[sample['@id']] = sample
                if sample['@id'] not in samples_seen:
                    samples_seen.add(sample['@id'])
        if 'donors' in full_fs:
            print('Getting donors')
            await props.portal_cache.async_batch_get(
                full_fs['donors'],
                async_portal_api,
            )
            for donor in full_fs['donors']:
                if donor not in donors_seen:
                    donors_seen.add(donor)
    await reset_async_portal_api(async_portal_api)
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
                    props.portal_cache.local[f]['s3_uri']
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


async def add_fields_to_row(item: Dict[str, Any], fields: List[str], row: List[Any], name: str, portal_cache: PortalCache, api: AsyncIgvfApi):
    for field in fields:
        value = None
        if field == 'type':
            value = item['@type'][0]
        elif field == 'id':
            value = item['@id']
        elif name == 'files' and field == 'file_name':
            value = item['s3_uri'].split('/')[-1]
        elif name == 'files' and field == 'file_md5sum':
            value = item['md5sum']
        elif name == 'files' and field == 'reference_assembly':
            value = item.get('assembly', '')
        elif name == 'files' and field == 'sequencing_platform':
            at_id = value = item.get('sequencing_platform', '')
            if at_id:
                value = (
                    await portal_cache.async_batch_get(
                        [at_id],
                        api,
                    )
                )[at_id]['term_name']
        elif name == 'file_sets' and field == 'lab':
            at_id = item['lab']
            value = (
                await portal_cache.async_batch_get(
                    [at_id],
                    api,
                )
            )[at_id]['title']
        elif name == 'file_sets' and field == 'award':
            value = parse_accession_from_at_ids(
                [
                    item['award']
                ]
            )[0]
        elif name == 'donors' and field == 'organism_type':
            value = item.get('taxa', '')
        elif name == 'donors' and field == 'phenotypic_sex':
            value = item.get('sex', '')
        elif name == 'donors' and field == 'reported_ethnicity':
            value = item.get('ethnicities', '')
        elif name == 'donors' and field == 'phenotypic_features':
            at_ids = value = item.get('phenotypic_features', '')
            if at_ids:
                features = await portal_cache.async_batch_get(
                    at_ids,
                    api,
                )
                term_at_ids = [
                    v['feature']
                    for k, v in features.items()
                    if 'feature' in v
                ]
                terms = await portal_cache.async_batch_get(
                    term_at_ids,
                    api,
                )
                term_names = [
                    v['term_name']
                    for k, v in terms.items()
                    if 'term_name' in v
                ]
                value = list(sorted(set(term_names)))
        elif name == 'samples' and field == 'biosample_type':
            value = item.get('classifications', '')
        elif name == 'samples' and field == 'donor_age_at_collection_unit_upper_bound':
            value = item.get('upper_bound_age', '')
        elif name == 'samples' and field == 'donor_age_at_collection_unit_lower_bound':
            value = item.get('lower_bound_age', '')
        elif name == 'samples' and field == 'donor_age_at_collection_unit':
            value = item.get('age_units', '')
        elif name == 'samples' and field == 'sample_terms':
            at_ids = value = item.get('sample_terms', '')
            if at_ids:
                sample_terms = await portal_cache.async_batch_get(
                    at_ids,
                    api,
                )
                term_names = [
                    v['term_name']
                    for k, v in sample_terms.items()
                    if 'term_name' in v
                ]
                value = list(sorted(set(term_names)))
        elif name == 'samples' and field == 'targeted_sample_term':
            at_id = value = item.get('targeted_sample_term', '')
            if at_id:
                value = (
                    await portal_cache.async_batch_get(
                        [at_id],
                        api,
                    )
                )[at_id]['term_name']
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


async def make_data_tables(metadata: Dict[str, Any], metadata_props: MetadataProps, destination_bucket: str, portal_ui_url: str) -> Dict[str, Any]:
    portal_cache = metadata_props.portal_cache
    api = portal_cache.props.async_portal_api()
    cache = portal_cache.local
    file_headers = ['file_id', 'file_path', 'igvf_portal_url'] + FILE_FIELDS
    files_tsv = '\t'.join(file_headers)
    for f in metadata['seen']['files']:
        full_file = cache[f]
        row = [
            full_file['accession'],
            make_gs_file_path_from_s3_uri(
                destination_bucket,
                full_file['s3_uri']
            ),
            portal_ui_url + full_file['@id'],
        ]
        await add_fields_to_row(full_file, FILE_FIELDS, row, 'files', portal_cache, api)
        files_tsv = files_tsv + '\n' + '\t'.join(row)

    file_set_headers = ['file_set_id', 'igvf_portal_url'] + FILE_SET_FIELDS
    file_sets_tsv = '\t'.join(file_set_headers)
    for fs in metadata['seen']['file_sets']:
        full_fs = cache[fs]
        row = [
            full_fs['accession'],
            portal_ui_url + full_fs['@id'],
        ]
        await add_fields_to_row(full_fs, FILE_SET_FIELDS, row, 'file_sets', portal_cache, api)
        file_sets_tsv = file_sets_tsv + '\n' + '\t'.join(row)

    sample_headers = ['sample_id', 'igvf_portal_url'] + SAMPLE_FIELDS
    samples_tsv = '\t'.join(sample_headers)
    for s in metadata['seen']['samples']:
        full_s = cache[s]
        row = [
            full_s['accession'],
            portal_ui_url + full_s['@id'],
        ]
        await add_fields_to_row(full_s, SAMPLE_FIELDS, row, 'samples', portal_cache, api)
        samples_tsv = samples_tsv + '\n' + '\t'.join(row)

    donor_headers = ['donor_id', 'igvf_portal_url'] + DONOR_FIELDS
    donors_tsv = '\t'.join(donor_headers)
    for d in metadata['seen']['donors']:
        full_d = cache[d]
        row = [
            full_d['accession'],
            portal_ui_url + full_d['@id'],
        ]
        await add_fields_to_row(full_d, DONOR_FIELDS, row, 'donors', portal_cache, api)
        donors_tsv = donors_tsv + '\n' + '\t'.join(row)
    await reset_async_portal_api(api)
    return {
        'files': files_tsv,
        'file_sets': file_sets_tsv,
        'samples': samples_tsv,
        'donors': donors_tsv,
    }
