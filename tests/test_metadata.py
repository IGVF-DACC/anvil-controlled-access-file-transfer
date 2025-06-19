import asyncio
import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def context(scope='function'):
    from igvf_async_client import AsyncIgvfApi
    from metadata import MetadataProps
    from cache import PortalCache
    from cache import PortalCacheProps
    return {
        'SOME-DUL': {
            'metadata_props': MetadataProps(
                dul='SOME-DUL',
                initial_files_query=(
                    'https://api.data.igvf.org/search/'
                    '?type=File'
                    '&file_set.data_use_limitation_summaries=HMB-MDS'
                    '&file_set.controlled_access=true'
                    '&status=released'
                    '&frame=object'
                    '&limit=all'
                ),
                portal_cache=PortalCache(
                    props=PortalCacheProps(
                        url='https://api.data.igvf.org',
                        async_portal_api=AsyncIgvfApi,
                    )
                )
            ),
            'name': 'igvf-anvil-some-dul',
            'project_id': 'PROJECT-123',
            'manifest_bucket': 'some-manifest-bucket',
            'destination_bucket': 'some-destination-bucket',
            'sleep_time_seconds': 120,
            'workspace_namespace': 'some_workspace_namespace',
            'workspace_name': 'some_workspace_name',
            'overwrite_tsvs': False,
        }
    }


@pytest.mark.asyncio()
async def test_metadata_collect_metadata(context):
    from metadata import collect_metadata
    metadata_props = context['SOME-DUL']['metadata_props']
    metadata = await collect_metadata(metadata_props)
    actual = metadata['seen']
    expected = {
        "files": [
            "/configuration-files/IGVFFI6594MPVX/",
            "/configuration-files/IGVFFI7971ZCIE/",
            "/matrix-files/IGVFFI8142SBEI/",
            "/sequence-files/IGVFFI1517VBJH/",
            "/sequence-files/IGVFFI1915BHZZ/",
            "/sequence-files/IGVFFI2440LTBO/",
            "/sequence-files/IGVFFI8905WBYU/",
            "/sequence-files/IGVFFI9831SEGU/",
            "/sequence-files/IGVFFI9846ECMS/",
            "/tabular-files/IGVFFI9764GKVR/"
        ],
        "file_sets": [
            "/analysis-sets/IGVFDS0718WXPI/",
            "/measurement-sets/IGVFDS2259DWKL/",
            "/measurement-sets/IGVFDS2534CJYA/"
        ],
        "samples": [
            "/in-vitro-systems/IGVFSM7625PGWV/"
        ],
        "donors": [
            "/human-donors/IGVFDO1048RJRX/",
            "/human-donors/IGVFDO3401QPKA/",
            "/human-donors/IGVFDO5812FFJI/",
            "/human-donors/IGVFDO7463WRZR/",
            "/human-donors/IGVFDO8630TWDU/",
            "/human-donors/IGVFDO9637UZMV/"
        ]
    }
    assert actual == expected


@pytest.mark.asyncio()
async def test_metadata_make_data_tables(context):
    from metadata import make_data_tables
    from .expected import CACHE, METADATA
    metadata_props = context['SOME-DUL']['metadata_props']
    metadata_props.portal_cache.local = CACHE
    expected = {'files': 'file_id\tfile_path\tigvf_portal_url\ttype\tsummary\tassay_titles\tcontent_type\tfile_name\tfile_format\tfile_format_type\tfile_size\tfile_md5sum\tfile_set\tseqspecs\tworkflow\tderived_from\treference_assembly\tcell_type_annotation\tflowcell_id\tillumina_read_type\tlane\tmean_read_length\tsequencing_kit\tsequencing_platform\tsequencing_run\ttranscriptome_annotation\nIGVFFI6594MPVX\tgs://some_destination_bucket/2025/03/18/775b9e95-6c30-4f1f-b772-428745bb0c53/IGVFFI6594MPVX.yaml.gz\thttps://data.igvf.org/configuration-files/IGVFFI6594MPVX/\tConfigurationFile\tseqspec of IGVFFI9831SEGU, IGVFFI8905WBYU, IGVFFI2440LTBO\t["SHARE-seq"]\tseqspec\tIGVFFI6594MPVX.yaml.gz\tyaml\t\t1242\t8336c45ce32429d0f31dd3b9413c0de4\tIGVFDS2534CJYA\t\t\t\t\t\t\t\t\t\t\t\t\t\nIGVFFI7971ZCIE\tgs://some_destination_bucket/2025/03/18/9ca8a328-ee28-4b84-a299-12a91a14e0d2/IGVFFI7971ZCIE.yaml.gz\thttps://data.igvf.org/configuration-files/IGVFFI7971ZCIE/\tConfigurationFile\tseqspec of IGVFFI9846ECMS, IGVFFI1915BHZZ, IGVFFI1517VBJH\t["SHARE-seq"]\tseqspec\tIGVFFI7971ZCIE.yaml.gz\tyaml\t\t1216\t62545fbf3d2c85d50cad140bb8c87e2e\tIGVFDS2259DWKL\t\t\t\t\t\t\t\t\t\t\t\t\t\nIGVFFI8142SBEI\tgs://some_destination_bucket/2025/03/06/3a7b359e-39f7-4240-a6ee-703e6299455b/IGVFFI8142SBEI.h5ad\thttps://data.igvf.org/matrix-files/IGVFFI8142SBEI/\tMatrixFile\tunfiltered cell by gene in sparse gene count matrix\t["SHARE-seq"]\tsparse gene count matrix\tIGVFFI8142SBEI.h5ad\th5ad\t\t63881699\t6672db7b863aa8283fc35420d591eef5\tIGVFDS0718WXPI\t\t/workflows/IGVFWF0507XZOR/\t["IGVFFI9831SEGU", "IGVFFI8905WBYU"]\t\t\t\t\t\t\t\t\t\t\nIGVFFI1517VBJH\tgs://some_destination_bucket/2025/03/14/94b93923-9a1d-441a-af9c-3cc4fec3f153/IGVFFI1517VBJH.fastq.gz\thttps://data.igvf.org/sequence-files/IGVFFI1517VBJH/\tSequenceFile\tI1 reads from sequencing run 1\t["SHARE-seq"]\treads\tIGVFFI1517VBJH.fastq.gz\tfastq\t\t5206068496\t46da9565e75c16a169d881d83901e756\tIGVFDS2259DWKL\t["IGVFFI7971ZCIE"]\t\t\t\t\t\tI1\t\t32.0\tNovaSeq 6000 S4 Reagent Kit v1.5\tIllumina NovaSeq 6000\t1\t\nIGVFFI1915BHZZ\tgs://some_destination_bucket/2025/02/20/603c456d-23ca-4e4c-9db9-b01a33929f3a/IGVFFI1915BHZZ.fastq.gz\thttps://data.igvf.org/sequence-files/IGVFFI1915BHZZ/\tSequenceFile\tR2 reads from sequencing run 1\t["SHARE-seq"]\treads\tIGVFFI1915BHZZ.fastq.gz\tfastq\t\t10279868253\tf4981ce2ce23080f52427a1aaad3c145\tIGVFDS2259DWKL\t["IGVFFI7971ZCIE"]\t\t\t\t\t\tR2\t\t49.06\tNovaSeq 6000 S4 Reagent Kit v1.5\tIllumina NovaSeq 6000\t1\t\nIGVFFI2440LTBO\tgs://some_destination_bucket/2025/03/14/bda1245f-0708-46dc-83b0-519913e90e47/IGVFFI2440LTBO.fastq.gz\thttps://data.igvf.org/sequence-files/IGVFFI2440LTBO/\tSequenceFile\tI1 reads from sequencing run 1\t["SHARE-seq"]\treads\tIGVFFI2440LTBO.fastq.gz\tfastq\t\t6719075164\tad6d9107e67228b2037da34dbacd634a\tIGVFDS2534CJYA\t["IGVFFI6594MPVX"]\t\t\t\t\t\tI1\t\t32.0\tNovaSeq 6000 S4 Reagent Kit v1.5\tIllumina NovaSeq 6000\t1\t\nIGVFFI8905WBYU\tgs://some_destination_bucket/2025/02/20/dff839b9-e8c3-4fd5-adc8-eabf2873b34c/IGVFFI8905WBYU.fastq.gz\thttps://data.igvf.org/sequence-files/IGVFFI8905WBYU/\tSequenceFile\tR2 reads from sequencing run 1\t["SHARE-seq"]\treads\tIGVFFI8905WBYU.fastq.gz\tfastq\t\t10730781494\te1969d8435c7e1b059ab3e5df6944032\tIGVFDS2534CJYA\t["IGVFFI6594MPVX"]\t\t\t\t\t\tR2\t\t50.0\tNovaSeq 6000 S4 Reagent Kit v1.5\tIllumina NovaSeq 6000\t1\t\nIGVFFI9831SEGU\tgs://some_destination_bucket/2025/02/20/ac23fc63-362d-4539-8477-486980ffec9e/IGVFFI9831SEGU.fastq.gz\thttps://data.igvf.org/sequence-files/IGVFFI9831SEGU/\tSequenceFile\tR1 reads from sequencing run 1\t["SHARE-seq"]\treads\tIGVFFI9831SEGU.fastq.gz\tfastq\t\t10787849140\t480e0e47270cdf96cb856fa0d8643762\tIGVFDS2534CJYA\t["IGVFFI6594MPVX"]\t\t\t\t\t\tR1\t\t50.0\tNovaSeq 6000 S4 Reagent Kit v1.5\tIllumina NovaSeq 6000\t1\t\nIGVFFI9846ECMS\tgs://some_destination_bucket/2025/02/20/084c1c10-cf73-446f-940d-62c05a611b8b/IGVFFI9846ECMS.fastq.gz\thttps://data.igvf.org/sequence-files/IGVFFI9846ECMS/\tSequenceFile\tR1 reads from sequencing run 1\t["SHARE-seq"]\treads\tIGVFFI9846ECMS.fastq.gz\tfastq\t\t9872707806\t20791df01e1ea5aaa15ac135aa01eb9e\tIGVFDS2259DWKL\t["IGVFFI7971ZCIE"]\t\t\t\t\t\tR1\t\t49.06\tNovaSeq 6000 S4 Reagent Kit v1.5\tIllumina NovaSeq 6000\t1\t\nIGVFFI9764GKVR\tgs://some_destination_bucket/2025/03/06/cc1d12c6-4a61-401c-bbb8-21314b1d6cd0/IGVFFI9764GKVR.tsv.gz\thttps://data.igvf.org/tabular-files/IGVFFI9764GKVR/\tTabularFile\tfragments\t["SHARE-seq"]\tfragments\tIGVFFI9764GKVR.tsv.gz\ttsv\t\t1206512769\t17dcfe73b68c88c636982c553da9b3ea\tIGVFDS0718WXPI\t\t/workflows/IGVFWF0507XZOR/\t["IGVFFI9846ECMS", "IGVFFI1915BHZZ"]\t\t\t\t\t\t\t\t\t\t', 'file_sets': 'file_set_id\tigvf_portal_url\ttype\tassay_term\tassay_titles\tfiles\tassociated_phenotypes\tauxiliary_sets\taverage_guide_coverage\taverage_insert_size\taward\tbarcode_map\tconstruct_library_sets\tcontrol_file_sets\texon\tfile_set_type\tguide_type\tinput_file_sets\tlab\tpreferred_assay_title\tsample_summary\tsamples\tscope\tselection_criteria\tsequencing_library_types\tsmall_scale_gene_list\tsmall_scale_loci_list\tsummary\ttargeted_genes\nIGVFDS0718WXPI\thttps://data.igvf.org/analysis-sets/IGVFDS0718WXPI/\tAnalysisSet\t\t["SHARE-seq"]\t["IGVFFI8142SBEI", "IGVFFI9764GKVR"]\t\t\t\t\tA foundational resource of functional elements, TF footprints and gene regulatory interactions\t\t\t\t\tintermediate analysis\t\t["IGVFDS2259DWKL", "IGVFDS2534CJYA"]\tJason Buenrostro, Broad\t\tHomo sapiens colon epithelial cell organoid induced to colon, at 7 day(s) post change\t["IGVFSM7625PGWV"]\t\t\t\t\t\tsingle-cell ATAC-seq (SHARE-seq), single-cell RNA sequencing assay (SHARE-seq)\t\nIGVFDS2259DWKL\thttps://data.igvf.org/measurement-sets/IGVFDS2259DWKL/\tMeasurementSet\t/assay-terms/OBI_0002764/\t\t["IGVFFI7971ZCIE", "IGVFFI9846ECMS", "IGVFFI1915BHZZ", "IGVFFI1517VBJH"]\t\t\t\t\tA foundational resource of functional elements, TF footprints and gene regulatory interactions\t\t\t\t\texperimental data\t\t\tJason Buenrostro, Broad\tSHARE-seq\t\t["IGVFSM7625PGWV"]\t\t\t\t\t\tsingle-cell ATAC-seq (SHARE-seq)\t\nIGVFDS2534CJYA\thttps://data.igvf.org/measurement-sets/IGVFDS2534CJYA/\tMeasurementSet\t/assay-terms/OBI_0002631/\t\t["IGVFFI6594MPVX", "IGVFFI9831SEGU", "IGVFFI8905WBYU", "IGVFFI2440LTBO"]\t\t\t\t\tA foundational resource of functional elements, TF footprints and gene regulatory interactions\t\t\t\t\texperimental data\t\t\tJason Buenrostro, Broad\tSHARE-seq\t\t["IGVFSM7625PGWV"]\t\t\t\t\t\tsingle-cell RNA sequencing assay (SHARE-seq)\t', 'samples': 'sample_id\tigvf_portal_url\ttype\tsummary\tsample_terms\tmodifications\ttargeted_sample_term\tbiosample_type\tmultiplexed_samples\tpooled_from\tdonors\tconstruct_library_sets\tbiosample_qualifiers\tembryonic\tsorted_fractions\tdonor_age_at_collection_unit_upper_bound\tdonor_age_at_collection_unit_lower_bound\tdonor_age_at_collection_unit\tmoi\nIGVFSM7625PGWV\thttps://data.igvf.org/in-vitro-systems/IGVFSM7625PGWV/\tInVitroSystem\tHomo sapiens colon epithelial cell organoid induced to colon for 7 days\t["/sample-terms/CL_0011108/"]\t\t/sample-terms/UBERON_0001155/\t["organoid"]\t\t\t["IGVFDO1048RJRX", "IGVFDO3401QPKA", "IGVFDO7463WRZR", "IGVFDO8630TWDU", "IGVFDO5812FFJI", "IGVFDO9637UZMV"]\t\t\tFalse\t[]\t\t\t\t', 'donors': 'donor_id\tigvf_portal_url\ttype\treported_ethnicity\tphenotypic_features\tphenotypic_sex\torganism_type\nIGVFDO1048RJRX\thttps://data.igvf.org/human-donors/IGVFDO1048RJRX/\tHumanDonor\t\t\tunspecified\tHomo sapiens\nIGVFDO3401QPKA\thttps://data.igvf.org/human-donors/IGVFDO3401QPKA/\tHumanDonor\t\t\tunspecified\tHomo sapiens\nIGVFDO5812FFJI\thttps://data.igvf.org/human-donors/IGVFDO5812FFJI/\tHumanDonor\t\t\tunspecified\tHomo sapiens\nIGVFDO7463WRZR\thttps://data.igvf.org/human-donors/IGVFDO7463WRZR/\tHumanDonor\t\t\tunspecified\tHomo sapiens\nIGVFDO8630TWDU\thttps://data.igvf.org/human-donors/IGVFDO8630TWDU/\tHumanDonor\t\t\tunspecified\tHomo sapiens\nIGVFDO9637UZMV\thttps://data.igvf.org/human-donors/IGVFDO9637UZMV/\tHumanDonor\t\t\tunspecified\tHomo sapiens'}
    actual = await make_data_tables(METADATA, metadata_props, 'some_destination_bucket', 'https://data.igvf.org')
    for k, v in actual.items():
        _item = {k: v}
        assert expected[k] == v, f'Actual: {_item}'
    assert expected == actual, f'{actual}'
