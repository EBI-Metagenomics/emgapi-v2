import csv
from pathlib import Path

import pytest
from django.core.management import call_command

from analyses.models import Analysis, Assembly, Run, Sample, Study
from ena.models import Sample as ENASample
from ena.models import Study as ENAStudy


def create_unknown_run(
    run_accession: str,
    *,
    library_strategy: str = "",
    library_source: str = "",
    scientific_name: str = "",
) -> Run:
    ena_study = ENAStudy.objects.create(accession=f"ERP{run_accession[-6:]}")
    study = Study.objects.create(ena_study=ena_study, title=f"Study {run_accession}")
    ena_sample = ENASample.objects.create(
        accession=f"ERS{run_accession[-6:]}", study=ena_study
    )
    sample = Sample.objects.create(ena_study=ena_study, ena_sample=ena_sample)
    run = Run.objects.create(
        study=study,
        ena_study=ena_study,
        sample=sample,
        ena_accessions=[run_accession],
        experiment_type=Run.ExperimentTypes.UNKNOWN,
        metadata={
            Run.CommonMetadataKeys.LIBRARY_STRATEGY: library_strategy,
            Run.CommonMetadataKeys.LIBRARY_SOURCE: library_source,
            Run.CommonMetadataKeys.SCIENTIFIC_NAME: scientific_name,
        },
    )
    return run


def create_analysis(run: Run, experiment_type: str) -> Analysis:
    return Analysis.objects.create(
        study=run.study,
        ena_study=run.ena_study,
        sample=run.sample,
        run=run,
        experiment_type=experiment_type,
    )


def run_command(output_tsv: Path):
    call_command("determine_unknown_run_experiment_types", output_tsv=str(output_tsv))
    with output_tsv.open() as report_file:
        return list(csv.DictReader(report_file, delimiter="\t"))


@pytest.mark.django_db
def test_command_sets_run_type_from_existing_analysis_and_inherits_to_unknown_analyses(
    tmp_path,
):
    run = create_unknown_run("ERR000001")
    typed_analysis = create_analysis(run, Analysis.ExperimentTypes.AMPLICON)
    unknown_analysis = create_analysis(run, Analysis.ExperimentTypes.UNKNOWN)

    rows = run_command(tmp_path / "report.tsv")

    run.refresh_from_db()
    typed_analysis.refresh_from_db()
    unknown_analysis.refresh_from_db()

    assert run.experiment_type == Run.ExperimentTypes.AMPLICON
    assert typed_analysis.experiment_type == Analysis.ExperimentTypes.AMPLICON
    assert unknown_analysis.experiment_type == Analysis.ExperimentTypes.AMPLICON
    assert rows[0]["run_accession"] == "ERR000001"
    assert rows[0]["status"] == "changed"
    assert rows[0]["reason"] == "analysis_experiment_type"


@pytest.mark.django_db
def test_command_logs_error_for_conflicting_analysis_types_and_leaves_run_unknown(
    tmp_path, caplog
):
    run = create_unknown_run("ERR000002")
    create_analysis(run, Analysis.ExperimentTypes.AMPLICON)
    create_analysis(run, Analysis.ExperimentTypes.METAGENOMIC)

    rows = run_command(tmp_path / "report.tsv")

    run.refresh_from_db()
    assert run.experiment_type == Run.ExperimentTypes.UNKNOWN
    assert "conflicting experiment types" in caplog.text
    assert "Could not determine experiment type for run ERR000002" in caplog.text
    assert rows[0]["status"] == "unresolved"
    assert rows[0]["reason"] == "conflicting_analysis_experiment_types"


@pytest.mark.django_db
def test_command_sets_assembly_attached_run_to_metagenomic(tmp_path):
    run = create_unknown_run("ERR000003")
    assembly = Assembly.objects.create(ena_study=run.ena_study, reads_study=run.study)
    assembly.runs.add(run)
    analysis = create_analysis(run, Analysis.ExperimentTypes.UNKNOWN)

    rows = run_command(tmp_path / "report.tsv")

    run.refresh_from_db()
    analysis.refresh_from_db()
    assert run.experiment_type == Run.ExperimentTypes.METAGENOMIC
    assert analysis.experiment_type == Analysis.ExperimentTypes.METAGENOMIC
    assert rows[0]["reason"] == "assembly_attached"


@pytest.mark.django_db
@pytest.mark.parametrize(
    (
        "run_accession",
        "library_strategy",
        "library_source",
        "scientific_name",
        "expected_type",
    ),
    [
        (
            "ERR000004",
            "WGS",
            "METAGENOMIC",
            "marine metagenome",
            Run.ExperimentTypes.METAGENOMIC,
        ),
        (
            "ERR000005",
            "AMPLICON",
            "METAGENOMIC",
            "marine metagenome",
            Run.ExperimentTypes.AMPLICON,
        ),
        (
            "ERR000006",
            "AMPLICON",
            "GENOMIC",
            "marine metagenome",
            Run.ExperimentTypes.AMPLICON,
        ),
    ],
)
def test_command_sets_raw_read_and_amplicon_run_types_from_metadata(
    tmp_path,
    run_accession,
    library_strategy,
    library_source,
    scientific_name,
    expected_type,
):
    run = create_unknown_run(
        run_accession,
        library_strategy=library_strategy,
        library_source=library_source,
        scientific_name=scientific_name,
    )

    rows = run_command(tmp_path / f"{run_accession}.tsv")

    run.refresh_from_db()
    assert run.experiment_type == expected_type
    assert rows[0]["status"] == "changed"
    assert rows[0]["reason"] == "ena_metadata"
    assert rows[0]["library_source"] == library_source


@pytest.mark.django_db
def test_command_warns_and_reports_run_that_remains_unknown(tmp_path, caplog):
    run = create_unknown_run(
        "ERR000007",
        library_strategy="AMPLICON",
        library_source="GENOMIC",
        scientific_name="Homo sapiens",
    )

    rows = run_command(tmp_path / "report.tsv")

    run.refresh_from_db()
    assert run.experiment_type == Run.ExperimentTypes.UNKNOWN
    assert "Could not determine experiment type for run ERR000007" in caplog.text
    assert rows[0]["status"] == "unresolved"
    assert rows[0]["reason"] == "ena_metadata"


@pytest.mark.django_db
def test_command_ignores_runs_that_are_not_unknown(tmp_path):
    run = create_unknown_run("ERR000008")
    run.experiment_type = Run.ExperimentTypes.METAGENOMIC
    run.save()

    rows = run_command(tmp_path / "report.tsv")

    assert rows == []
