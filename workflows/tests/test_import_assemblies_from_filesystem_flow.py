import gzip
import json
from pathlib import Path
from unittest.mock import patch

import pytest
import pandera.pandas as pa

import analyses.models
from workflows.data_io_utils.miassembler_utils import (
    miassembler_run_output_dir,
)
from workflows.flows.assemble_study_tasks.miassembler_reports import (
    load_assembled_runs_report,
)
from workflows.flows.import_assemblies_from_filesystem_flow import (
    import_assemblies_from_filesystem_flow,
)


def write_contigs(path: Path) -> None:
    """
    Write a tiny gzipped FASTA file at the expected miassembler contigs path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as file_handle:
        file_handle.write(">contig_1\nACGT\n>contig_2\nTGCA\n")


def make_miassembler_output(
    output_dir: Path,
    study_accession: str,
    assembled_runs: list[tuple[str, str, str]],
    qc_failed_runs: dict[str, str] | None = None,
    include_coverage: bool = True,
    include_contigs: bool = True,
) -> Path:
    """
    Create the subset of miassembler output files needed by the import flow tests.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    with (output_dir / "assembled_runs.csv").open("w") as file_handle:
        for run_accession, assembler_name, assembler_version in assembled_runs:
            file_handle.write(f"{run_accession},{assembler_name},{assembler_version}\n")

            assembly_dir = (
                miassembler_run_output_dir(output_dir, study_accession, run_accession)
                / "assembly"
                / assembler_name
                / assembler_version
            )
            (assembly_dir / "coverage").mkdir(parents=True, exist_ok=True)
            if include_coverage:
                with (
                    assembly_dir / "coverage" / f"{run_accession}_coverage.json"
                ).open("w") as coverage_handle:
                    json.dump(
                        {"coverage": 0.04760503915318373, "coverage_depth": 273.694},
                        coverage_handle,
                    )
            if include_contigs:
                write_contigs(assembly_dir / f"{run_accession}_cleaned.contigs.fa.gz")

    if qc_failed_runs:
        with (output_dir / "qc_failed_runs.csv").open("w") as file_handle:
            for run_accession, reason in qc_failed_runs.items():
                file_handle.write(f"{run_accession},{reason}\n")

    return output_dir


def run_import_flow(
    output_dir: Path,
    study_accession: str,
    ena_assembly_accessions: dict[str, str] | None = None,
    **kwargs,
) -> dict[str, list[int] | list[dict[str, str]]]:
    """Invoke the filesystem import flow with the shared test defaults."""
    if ena_assembly_accessions is None:
        ena_assembly_accessions = {
            line.split(",", maxsplit=1)[0]: f"ERZ{index:06d}"
            for index, line in enumerate(
                (output_dir / "assembled_runs.csv").read_text().splitlines(),
                start=1,
            )
            if line
        }

    def fetch_ena_assemblies(accession: str, limit: int) -> list[str]:
        mgnify_study = analyses.models.Study.objects.get(ena_study__accession=accession)
        accessions = []
        for run_accession, assembly_accession in ena_assembly_accessions.items():
            run = analyses.models.Run.objects.get(
                ena_accessions__contains=[run_accession]
            )
            assembly, _ = (
                analyses.models.Assembly.objects.get_or_create_for_run_and_sample(
                    run=run,
                    sample=run.sample,
                    reads_study=mgnify_study,
                    ena_study=mgnify_study.ena_study,
                    defaults={
                        "is_private": run.is_private,
                        "webin_submitter": run.webin_submitter,
                    },
                )
            )
            assembly.ena_accessions = [assembly_accession]
            assembly.metadata["generated_ftp"] = (
                f"ftp.sra.ebi.ac.uk/vol1/sequence/{assembly_accession}/contigs.fa.gz"
            )
            assembly.save()
            accessions.append(assembly_accession)
        return accessions

    def fetch_ena_assembly_records(
        study_accession: str,
        mgnify_study_accession: str,
        limit: int,
    ) -> list[dict[str, str]]:
        return [
            {
                "run_accession": run_accession,
                "analysis_alias": run_accession,
                "analysis_accession": assembly_accession,
                "generated_ftp": (
                    f"ftp.sra.ebi.ac.uk/vol1/sequence/"
                    f"{assembly_accession}/contigs.fa.gz"
                ),
            }
            for run_accession, assembly_accession in ena_assembly_accessions.items()
        ]

    with (
        patch(
            "workflows.flows.import_assemblies_from_filesystem_flow."
            "get_study_assemblies_from_ena",
            side_effect=fetch_ena_assemblies,
        ),
        patch(
            "workflows.flows.import_assemblies_from_filesystem_flow."
            "get_study_assembly_records_from_ena",
            side_effect=fetch_ena_assembly_records,
        ),
    ):
        return import_assemblies_from_filesystem_flow(
            study_accession=study_accession,
            nextflow_outdir=output_dir,
            fetch_read_runs_from_ena=False,
            **kwargs,
        )


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_rejects_empty_assembled_runs_report(
    prefect_harness,
    raw_reads_mgnify_study,
    tmp_path,
):
    output_dir = tmp_path / "miassembler"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "assembled_runs.csv").write_text("")

    with pytest.raises(
        ValueError,
        match="There are no assemblies to import, the assembled_runs.csv is empty.",
    ):
        run_import_flow(output_dir, raw_reads_mgnify_study.ena_study.accession)


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_treats_empty_qc_failed_report_as_none(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_read_run,
    assemblers,
    tmp_path,
):
    output_dir = make_miassembler_output(
        tmp_path / "miassembler",
        raw_reads_mgnify_study.ena_study.accession,
        [("SRR6180434", "metaspades", "3.15.5")],
    )
    (output_dir / "qc_failed_runs.csv").write_text("")

    summary = run_import_flow(output_dir, raw_reads_mgnify_study.ena_study.accession)

    assert summary["qc_failed"] == []
    assert len(summary["imported"]) == 1


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_imports_completed_and_reports_qc_failed(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_read_run,
    assemblers,
    tmp_path,
):
    output_dir = make_miassembler_output(
        tmp_path / "miassembler",
        raw_reads_mgnify_study.ena_study.accession,
        [("SRR6180434", "metaspades", "3.15.5")],
        {"SRR6180435": "filter_ratio_threshold_exceeded"},
    )

    captured_tables: list[list[dict[str, str]]] = []

    def capture_table_artifact(*args, **kwargs):
        if "table" in kwargs:
            captured_tables.append(kwargs["table"])
        elif len(args) >= 2:
            captured_tables.append(args[1])
        else:
            raise AssertionError("create_table_artifact was called without a table")

    with patch(
        "workflows.flows.import_assemblies_from_filesystem_flow.create_table_artifact",
        side_effect=capture_table_artifact,
    ):
        summary = run_import_flow(
            output_dir, raw_reads_mgnify_study.ena_study.accession
        )

    assert len(summary["imported"]) == 1
    assert len(summary["qc_failed"]) == 1
    assert summary["qc_failed"] == [
        {
            "run_accession": "SRR6180435",
            "reason": "filter_ratio_threshold_exceeded",
        }
    ]

    imported = analyses.models.Assembly.objects.get(id=summary["imported"][0])
    assert imported.status[imported.AssemblyStates.ASSEMBLY_UPLOADED]
    assert not imported.status[imported.AssemblyStates.ASSEMBLY_FAILED]
    assert imported.assembler.name == "metaspades"
    assert imported.assembler.version == "3.15.5"
    assert round(imported.metadata[imported.CommonMetadataKeys.COVERAGE], 4) == 0.0476
    assert (
        round(imported.metadata[imported.CommonMetadataKeys.COVERAGE_DEPTH], 3)
        == 273.694
    )
    assert imported.dir == str(
        miassembler_run_output_dir(
            output_dir,
            raw_reads_mgnify_study.ena_study.accession,
            "SRR6180434",
        )
    )

    assert not analyses.models.Assembly.objects.filter(
        runs__ena_accessions__contains=["SRR6180435"]
    ).exists()

    assert captured_tables
    report_rows = captured_tables[0]
    imported_row = next(
        row for row in report_rows if row["run_accession"] == "SRR6180434"
    )
    qc_failed_row = next(
        row for row in report_rows if row["run_accession"] == "SRR6180435"
    )
    assert imported_row["result"] == "imported"
    assert imported_row["assembly_id"] == summary["imported"][0]
    assert qc_failed_row == {
        "run_accession": "SRR6180435",
        "result": "qc_failed_not_imported",
        "assembly_id": "",
        "assembler_name": "",
        "assembler_version": "",
        "reason": "filter_ratio_threshold_exceeded",
    }


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_preserves_input_study_accession(
    prefect_harness,
    raw_read_ena_study,
    raw_reads_mgnify_study,
    raw_read_run,
    assemblers,
    tmp_path,
):
    alias_accession = "SRP398089"
    raw_read_ena_study.additional_accessions = [alias_accession]
    raw_read_ena_study.save()
    raw_reads_mgnify_study.inherit_accessions_from_related_ena_object("ena_study")

    output_dir = make_miassembler_output(
        tmp_path / "miassembler",
        alias_accession,
        [("SRR6180434", "metaspades", "3.15.5")],
    )

    summary = run_import_flow(output_dir, alias_accession)

    imported = analyses.models.Assembly.objects.get(id=summary["imported"][0])
    assert imported.dir == str(
        miassembler_run_output_dir(output_dir, alias_accession, "SRR6180434")
    )
    assert imported.status[imported.AssemblyStates.ASSEMBLY_UPLOADED]


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_requires_existing_run(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_read_run,
    assemblers,
    tmp_path,
):
    run = raw_read_run[0]
    run.delete()
    output_dir = make_miassembler_output(
        tmp_path / "miassembler",
        raw_reads_mgnify_study.ena_study.accession,
        [("SRR6180434", "metaspades", "3.15.5")],
    )

    with (
        patch(
            "workflows.flows.import_assemblies_from_filesystem_flow."
            "get_study_assemblies_from_ena",
            return_value=["ERZ000001"],
        ),
        patch(
            "workflows.flows.import_assemblies_from_filesystem_flow."
            "get_study_assembly_records_from_ena",
            return_value=[
                {
                    "run_accession": "SRR6180434",
                    "analysis_alias": "SRR6180434",
                    "analysis_accession": "ERZ000001",
                    "generated_ftp": (
                        "ftp.sra.ebi.ac.uk/vol1/sequence/ERZ000001/contigs.fa.gz"
                    ),
                }
            ],
        ),
    ):
        with pytest.raises(analyses.models.Run.DoesNotExist):
            import_assemblies_from_filesystem_flow(
                study_accession=raw_reads_mgnify_study.ena_study.accession,
                nextflow_outdir=output_dir,
                fetch_read_runs_from_ena=False,
            )


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_rejects_invalid_run_accession_in_report(
    prefect_harness,
    raw_reads_mgnify_study,
    tmp_path,
):
    output_dir = tmp_path / "miassembler"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "assembled_runs.csv").write_text("INVALID,metaspades,3.15.5\n")
    (output_dir / "qc_failed_runs.csv").write_text("")

    with pytest.raises(pa.errors.SchemaError, match="INVALID"):
        run_import_flow(output_dir, raw_reads_mgnify_study.ena_study.accession)


@pytest.mark.django_db
def test_load_assembled_runs_report_rejects_invalid_run_accession(tmp_path):
    report = tmp_path / "assembled_runs.csv"
    report.write_text("INVALID,metaspades,3.15.5\n")

    with pytest.raises(pa.errors.SchemaError, match="INVALID"):
        load_assembled_runs_report(report)


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_clears_blocked_on_retry(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_read_run,
    assemblers,
    tmp_path,
):
    output_dir = make_miassembler_output(
        tmp_path / "miassembler",
        raw_reads_mgnify_study.ena_study.accession,
        [("SRR6180434", "metaspades", "3.15.5")],
    )

    first_summary = run_import_flow(
        output_dir, raw_reads_mgnify_study.ena_study.accession
    )
    assembly = analyses.models.Assembly.objects.get(id=first_summary["imported"][0])
    assembly.mark_status(assembly.AssemblyStates.ASSEMBLY_BLOCKED)

    second_summary = run_import_flow(
        output_dir, raw_reads_mgnify_study.ena_study.accession
    )

    assert second_summary["imported"] == first_summary["imported"]
    assembly.refresh_from_db()
    assert assembly.status[assembly.AssemblyStates.ASSEMBLY_UPLOADED]
    assert not assembly.status[assembly.AssemblyStates.ASSEMBLY_BLOCKED]


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_fails_without_coverage_report(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_read_run,
    assemblers,
    tmp_path,
):
    output_dir = make_miassembler_output(
        tmp_path / "miassembler",
        raw_reads_mgnify_study.ena_study.accession,
        [("SRR6180434", "metaspades", "3.15.5")],
        include_coverage=False,
    )

    with pytest.raises(FileNotFoundError, match="coverage"):
        run_import_flow(output_dir, raw_reads_mgnify_study.ena_study.accession)

    assert not analyses.models.Assembly.objects.filter(
        runs__ena_accessions__contains=["SRR6180434"]
    ).exists()


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_fails_without_contigs(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_read_run,
    assemblers,
    tmp_path,
):
    output_dir = make_miassembler_output(
        tmp_path / "miassembler",
        raw_reads_mgnify_study.ena_study.accession,
        [("SRR6180434", "metaspades", "3.15.5")],
        include_contigs=False,
    )

    with pytest.raises(FileNotFoundError, match="contigs"):
        run_import_flow(output_dir, raw_reads_mgnify_study.ena_study.accession)

    assert not analyses.models.Assembly.objects.filter(
        runs__ena_accessions__contains=["SRR6180434"]
    ).exists()


@pytest.mark.django_db
def test_import_assemblies_from_filesystem_requires_assembly_in_ena(
    prefect_harness,
    raw_reads_mgnify_study,
    raw_read_run,
    assemblers,
    tmp_path,
):
    output_dir = make_miassembler_output(
        tmp_path / "miassembler",
        raw_reads_mgnify_study.ena_study.accession,
        [("SRR6180434", "metaspades", "3.15.5")],
    )

    with patch(
        "workflows.flows.import_assemblies_from_filesystem_flow."
        "import_completed_assembly",
    ) as mock_import_completed_assembly:
        with pytest.raises(ValueError, match="was not found in ENA"):
            run_import_flow(
                output_dir,
                raw_reads_mgnify_study.ena_study.accession,
                ena_assembly_accessions={},
            )

    mock_import_completed_assembly.assert_not_called()
