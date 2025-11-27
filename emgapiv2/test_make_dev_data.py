from unittest.mock import patch, Mock

import pytest
from django.core.management import call_command

from analyses.models import Biome, Run, Analysis
from workflows.flows.analyse_study_tasks.amplicon.import_completed_amplicon_analyses import (
    import_completed_analysis as import_completed_amplicon_analysis,
)


@pytest.fixture
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
def amplicon_analysis_with_downloads(
    mock_run_deployment,
    raw_reads_mgnify_study,
    raw_reads_mgnify_sample,
):
    sample = raw_reads_mgnify_sample[0]
    study = raw_reads_mgnify_study
    mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

    run = Run.objects.create(
        ena_accessions=["SRR1111111"],
        study=study,
        ena_study=study.ena_study,
        sample=sample,
        experiment_type=Run.ExperimentTypes.AMPLICON,
        metadata={
            Run.CommonMetadataKeys.FASTQ_FTPS: ["ftp://example.org/SRR1111111.fastq"]
        },
    )

    analysis = Analysis.objects.create(
        ena_study=study.ena_study,
        study=study,
        experiment_type=Run.ExperimentTypes.AMPLICON,
        sample=sample,
        run=run,
    )
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_COMPLETED)

    analysis.results_dir = "/app/data/tests/amplicon_v6_output/SRR1111111"
    analysis.metadata[analysis.KnownMetadataKeys.MARKER_GENE_SUMMARY] = {
        analysis.CLOSED_REFERENCE: {
            "marker_genes": {
                "ITS": {"Eukarya": {"read_count": 0, "majority_marker": False}},
                "LSU": {
                    "Archaea": {"read_count": 0, "majority_marker": False},
                    "Eukarya": {"read_count": 0, "majority_marker": False},
                    "Bacteria": {"read_count": 0, "majority_marker": False},
                },
                "SSU": {
                    "Archaea": {"read_count": 65, "majority_marker": True},
                    "Eukarya": {"read_count": 0, "majority_marker": True},
                    "Bacteria": {"read_count": 28655, "majority_marker": True},
                },
            }
        },
        analysis.ASV: {
            "amplified_regions": [
                {
                    "asv_count": 94,
                    "read_count": 16664,
                    "marker_gene": "16S",
                    "amplified_region": "V3-V4",
                }
            ]
        },
    }
    analysis.save()
    import_completed_amplicon_analysis(analysis)


@pytest.fixture
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
def assembly_analysis_with_downloads(mock_run_deployment, mgnify_assemblies_completed):
    assem = mgnify_assemblies_completed[0]
    assem.add_erz_accession(
        "ERZ857107"
    )  # n.b. does not correspond to this run in real ena

    study = assem.reads_study
    sample = assem.sample
    mock_run_deployment.return_value = Mock(id="mock-flow-run-id")

    analysis = Analysis.objects.create(
        ena_study=study.ena_study,
        study=study,
        experiment_type=Run.ExperimentTypes.ASSEMBLY,
        sample=sample,
        assembly=assem,
    )
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_COMPLETED)
    analysis.results_dir = "/app/data/tests/assembly_v6_output/ERP106708/ERZ857107"
    analysis.save()


# TODO: currently unused as download data fixtures are missing
# @pytest.fixture
# @patch("workflows.flows.analyse_study_tasks.copy_v6_pipeline_results.move_data")
# def rawreads_analysis_with_downloads(
#     mock_copy_flow, raw_reads_mgnify_study, raw_reads_mgnify_sample
# ):
#     sample = raw_reads_mgnify_sample[0]
#     study = raw_reads_mgnify_study
#
#     run = Run.objects.create(
#         ena_accessions=["SRR1111111"],
#         study=study,
#         ena_study=study.ena_study,
#         sample=sample,
#         experiment_type=Run.ExperimentTypes.METAGENOMIC,
#         metadata={
#             Run.CommonMetadataKeys.FASTQ_FTPS: ["ftp://example.org/SRR1111111.fastq"]
#         },
#     )
#
#     analysis = Analysis.objects.create(
#         ena_study=study.ena_study,
#         study=study,
#         experiment_type=Run.ExperimentTypes.METAGENOMIC,
#         sample=sample,
#         run=run,
#     )
#     analysis.mark_status(analysis.AnalysisStates.ANALYSIS_COMPLETED)
#
#     analysis.results_dir = "/app/data/tests/rawreads_v6_output/SRR1111111"
#     analysis.metadata[analysis.KnownMetadataKeys.MARKER_GENE_SUMMARY] = {
#         analysis.TAXONOMIES: {
#             "marker_genes": {
#                 "MOTUS": {"Eukarya": {"read_count": 0, "majority_marker": False}},
#                 "LSU": {
#                     "Archaea": {"read_count": 0, "majority_marker": False},
#                     "Eukarya": {"read_count": 0, "majority_marker": False},
#                     "Bacteria": {"read_count": 0, "majority_marker": False},
#                 },
#                 "SSU": {
#                     "Archaea": {"read_count": 65, "majority_marker": True},
#                     "Eukarya": {"read_count": 0, "majority_marker": True},
#                     "Bacteria": {"read_count": 28655, "majority_marker": True},
#                 },
#             }
#         },
#         analysis.FUNCTIONAL_ANNOTATION: {
#             "amplified_regions": [
#                 {
#                     "asv_count": 94,
#                     "read_count": 16664,
#                     "marker_gene": "16S",
#                     "amplified_region": "V3-V4",
#                 }
#             ]
#         },
#     }
#     analysis.save()
#     import_completed_rawreads_analysis(analysis)


@pytest.mark.dev_data_maker
@pytest.mark.django_db(transaction=True)
def test_make_dev_data(
    top_level_biomes,
    assemblers,
    study_downloads,
    mgnify_assemblies_completed,
    amplicon_analysis_with_downloads,
    assembly_analysis_with_downloads,
    raw_read_analyses,
    prefect_harness,
    geographic_locations,
    genome_catalogues,
    genomes,
    private_analysis_with_download,
    private_study_with_download,
    super_study,
    publication,
    mgnify_sample_lots_of_metadata,
    mgnify_study_full_metadata,
):
    """
    Stub test that just sets up fixtures and dumps them to JSON for using as dev data.
    """

    assert Biome.objects.count() == 4

    call_command("dumpdata", "-o", "dev-db.json", "--indent", "2")
