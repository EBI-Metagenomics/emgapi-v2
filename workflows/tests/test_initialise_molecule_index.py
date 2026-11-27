"""Tests for study batches copied by the initial molecule index flow."""

from copy import copy
from pathlib import Path
from unittest.mock import Mock, PropertyMock, patch

import pytest

from analyses.models import AnalysisMoleculeIndex
from workflows.flows.analysis.assembly.flows.initialise_molecule_index import (
    initialise_molecule_index,
)

NAMESPACE = "workflows.flows.analysis.assembly.flows.initialise_molecule_index"


@pytest.mark.django_db
@pytest.mark.parametrize("copy_success", [True, False])
@pytest.mark.parametrize("overwrite", [True, False])
def test_one_datamover_job_per_study(
    assembly_analysis, tmp_path, copy_success, overwrite
):
    analysis = assembly_analysis
    analysis.external_results_dir = "study/assembly/V6"
    analysis.save()
    analysis.mark_status(analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)
    second = copy(analysis)
    second.pk = None
    second.accession = "MGYA99999998"
    second.save()
    if overwrite:
        for item in (analysis, second):
            AnalysisMoleculeIndex.objects.create(analysis=item, chebi_ids=[])
    download = Mock(
        download_group="functional_annotation.rhea_reactions", path="rhea.tsv.gz"
    )
    state = Mock()
    state.is_completed.return_value = copy_success
    config = Mock(
        shared_filesystem_root_on_server=str(tmp_path),
        shared_filesystem_root_on_slurm="/cluster/shared",
        ftp_results_dir="/ftp/results",
        datamover_partition="datamover",
    )

    def mover(**kwargs):
        parameters = kwargs["parameters"]
        assert parameters["source"] == "/ftp/results/"
        assert parameters["target"] == f"/cluster/shared/rhea/{analysis.study_id}/"
        manifests = list((tmp_path / "rhea").glob("*.files"))
        assert len(manifests) == 1
        assert manifests[0].read_text() == "study/assembly/V6/rhea.tsv.gz\n"
        assert (
            f"--files-from=/cluster/shared/rhea/{manifests[0].name}"
            in parameters["move_command"]
        )
        assert kwargs["job_variables"] == {"partition": "datamover"}
        return Mock(id="copy-job", state=state)

    with (
        patch(NAMESPACE + ".get_run_logger"),
        patch(NAMESPACE + ".EMG_CONFIG.slurm", config),
        patch(
            "analyses.models.Analysis.downloads_as_objects",
            new_callable=PropertyMock,
            return_value=[download],
        ),
        patch(NAMESPACE + ".run_deployment", side_effect=mover) as deployment,
        patch(NAMESPACE + ".import_rhea_annotations", return_value=2) as importer,
    ):
        if copy_success:
            initialise_molecule_index.fn(
                tmp_path / "rhea", analysis.study.accession, overwrite=overwrite
            )
            assert {call.args[0] for call in importer.call_args_list} == {
                analysis.id,
                second.id,
            }
            assert importer.call_args.args[1] == (
                tmp_path
                / "rhea"
                / str(analysis.study_id)
                / "study/assembly/V6/rhea.tsv.gz"
            )
        else:
            with pytest.raises(RuntimeError, match="Rhea imports failed"):
                initialise_molecule_index.fn(tmp_path / "rhea", overwrite=overwrite)
            importer.assert_not_called()
        assert bool(list((tmp_path / "rhea").glob("*.files"))) is not copy_success
        deployment.assert_called_once()


@pytest.mark.django_db
@pytest.mark.parametrize(
    "excluded", ["private", "suppressed", "not_ready", "indexed", "v5"]
)
def test_initial_import_skips_ineligible_analyses(
    assembly_analysis, tmp_path, excluded
):
    analysis = assembly_analysis
    if excluded != "not_ready":
        analysis.mark_status(analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED)
    if excluded == "private":
        analysis.is_private = True
    elif excluded == "suppressed":
        analysis.is_suppressed = True
    elif excluded == "indexed":
        AnalysisMoleculeIndex.objects.create(analysis=analysis, chebi_ids=[])
    elif excluded == "v5":
        analysis.pipeline_version = "V5"
    analysis.save()
    with (
        patch(NAMESPACE + ".get_run_logger"),
        patch(
            NAMESPACE + ".EMG_CONFIG.slurm.shared_filesystem_root_on_server",
            str(tmp_path),
        ),
        patch(NAMESPACE + ".run_deployment") as deployment,
    ):
        initialise_molecule_index.fn(tmp_path / "rhea")
        deployment.assert_not_called()


def test_initial_import_rejects_nonshared_workdir():
    with (
        patch(NAMESPACE + ".get_run_logger"),
        pytest.raises(ValueError, match="absolute"),
    ):
        initialise_molecule_index.fn(Path("relative"))
