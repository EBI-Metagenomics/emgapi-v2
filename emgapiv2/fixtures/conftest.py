from unittest.mock import patch

import pytest

from analyses.models import Run, Analysis
from workflows.flows.analyse_study_tasks.import_completed_amplicon_analyses import (
    import_completed_analysis as import_completed_amplicon_analysis,
)
from workflows.flows.analyse_study_tasks.import_completed_assembly_analyses import (
    import_completed_assembly_analysis,
)


@pytest.fixture
@patch("workflows.flows.analyse_study_tasks.copy_v6_pipeline_results.move_data")
def amplicon_analysis_with_downloads(
    mock_copy_flow, raw_reads_mgnify_study, raw_reads_mgnify_sample, prefect_harness
):
    sample = raw_reads_mgnify_sample[0]
    study = raw_reads_mgnify_study

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
@patch("workflows.flows.analyse_study_tasks.copy_v6_pipeline_results.move_data")
def assembly_analysis_with_downloads(
    mock_copy_flow, mgnify_assemblies_completed, prefect_harness
):
    assem = mgnify_assemblies_completed[0]
    assem.add_erz_accession(
        "ERZ857107"
    )  # n.b. does not correspond to this run in real ena

    study = assem.reads_study
    sample = assem.sample

    analysis = Analysis.objects.create(
        ena_study=study.ena_study,
        study=study,
        experiment_type=Run.ExperimentTypes.ASSEMBLY,
        sample=sample,
        assembly=assem,
    )
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_COMPLETED)
    analysis.results_dir = "/app/data/tests/assembly_v6_output/ERZ857107/ERZ857107"
    analysis.save()
    import_completed_assembly_analysis(analysis)
