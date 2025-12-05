from pathlib import Path
from unittest.mock import patch, Mock

import django
import pytest

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadType,
    DownloadFileType,
)
from workflows.data_io_utils.mgnify_v6_utils.amplicon import import_qc, import_taxonomy
from workflows.data_io_utils.mgnify_v6_utils.assembly import AssemblyResultImporter
from workflows.data_io_utils.schemas import AssemblyResultSchema, MapResultSchema

django.setup()

import analyses.models as mg_models

versions = {"metaspades": "3.15.5", "spades": "3.15.5", "megahit": "1.2.9"}


@pytest.fixture
def raw_read_analyses(raw_read_run):
    mgyas = []
    for run in raw_read_run:
        mgya, _ = mg_models.Analysis.objects_and_annotations.get_or_create(
            run=run,
            sample=run.sample,
            study=run.study,
            ena_study_id=run.ena_study_id,
        )
        mgya.annotations[mg_models.Analysis.PFAMS] = [
            {"count": 1, "description": "PFAM1"}
        ]
        mgya.external_results_dir = f"analyses/{mgya.accession}"
        mgya.save()
        mgyas.append(mgya)

    mgyas[0].status[mg_models.Analysis.AnalysisStates.ANALYSIS_STARTED] = True
    mgyas[0].status[mg_models.Analysis.AnalysisStates.ANALYSIS_COMPLETED] = True
    mgyas[0].status[
        mg_models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    ] = True
    mgyas[1].status[mg_models.Analysis.AnalysisStates.ANALYSIS_STARTED] = True
    mgyas[0].save()
    mgyas[1].save()

    mgyas[0].results_dir = "/app/data/tests/amplicon_v6_output/SRR6180434"
    mgyas[0].save()

    import_qc(
        analysis=mgyas[0],
        dir_for_analysis=Path(mgyas[0].results_dir),
        allow_non_exist=False,
    )

    import_taxonomy(
        analysis=mgyas[0],
        dir_for_analysis=Path(mgyas[0].results_dir),
        source=mg_models.Analysis.TaxonomySources.SSU,
        allow_non_exist=False,
    )

    s = mgyas[0].study
    s.features.has_v6_analyses = True
    s.save()

    for mgya in mgyas:
        mgya.refresh_from_db()

    return mgyas


@pytest.fixture
def private_analysis_with_download(webin_private_study, private_run):
    run = private_run
    run.sample.studies.add(webin_private_study)

    private_analysis = mg_models.Analysis.objects.create(
        accession="MGYA00000888",
        study=webin_private_study,
        sample=run.sample,
        run=run,
        is_private=True,
        webin_submitter=webin_private_study.webin_submitter,
        ena_study=webin_private_study.ena_study,
    )

    private_analysis.external_results_dir = "MGYS/00/000/999/analyses/MGYA00000888"
    private_analysis.save()

    private_analysis.add_download(
        DownloadFile(
            download_type=DownloadType.SEQUENCE_DATA,
            file_type=DownloadFileType.FASTA,
            alias=f"{private_analysis.accession}_sequences.fasta",
            short_description="Private analysis sequences",
            long_description="Sequence data for private analysis",
            path="private_analysis_sequences.fasta",
            download_group="all.sequence_data.private",
        )
    )
    private_analysis.mark_status(
        private_analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED
    )
    return private_analysis


@pytest.fixture
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
def assembly_analysis(mock_run_deployment, mgnify_assemblies_completed):
    assem = mgnify_assemblies_completed[0]
    assem.add_erz_accession(
        "ERZ857107"
    )  # n.b. does not correspond to this run in real ena
    mock_run_deployment.return_value = Mock(id="mock-flow-run-id")
    study = assem.reads_study
    sample = assem.sample

    analysis = mg_models.Analysis.objects.create(
        ena_study=study.ena_study,
        study=study,
        experiment_type=mg_models.Run.ExperimentTypes.ASSEMBLY,
        sample=sample,
        assembly=assem,
    )
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_COMPLETED)
    analysis.results_dir = "/app/data/tests/assembly_v6_output/ERP106708/ERZ857107"
    analysis.save()
    return analysis


@pytest.fixture
@patch(
    "workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results.run_deployment"
)
def assembly_analysis_with_downloads(
    mock_run_deployment, prefect_harness, assembly_analysis
):
    schema = AssemblyResultSchema()
    mock_run_deployment.return_value = Mock(id="mock-flow-run-id")
    importer = AssemblyResultImporter(assembly_analysis)
    importer.import_results(
        schema=schema,
        base_path=Path(assembly_analysis.results_dir).parent,
        validate_first=True,
    )

    schema = MapResultSchema()
    importer.import_results(
        schema=schema,
        base_path=Path(assembly_analysis.results_dir).parent / "map",
        validate_first=True,
    )
    return assembly_analysis
