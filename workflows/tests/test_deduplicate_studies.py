import json

import pytest
from prefect.artifacts import Artifact

import ena.models
from analyses.models import (
    Analysis,
    Assembly,
    Publication,
    Run,
    Sample,
    Study,
    StudyPublication,
    SuperStudy,
    SuperStudyStudy,
)
from workflows.flows.housekeeping.deduplicate_studies import (
    deduplicate_studies,
    find_duplicated_studies,
)
from workflows.models import AssemblyAnalysisBatch


def _create_study_pair():
    ena_study = ena.models.Study.objects.create(
        accession="PRJOLD1",
        additional_accessions=["ERPCHAIN"],
        title="Canonical ENA",
    )
    study_old = Study.objects.create(
        ena_study=ena_study,
        title="Canonical study",
        metadata={"keep": "me"},
        results_dir="/work/original",
    )
    study_new = Study.objects.create(
        ena_study=ena_study,
        title="",
        metadata={"duplicate": "value"},
        external_results_dir="/work/duplicate",
    )
    study_old.ena_accessions = ["ERPCHAIN", "PRJOLD1"]
    study_old.save()
    study_new.ena_accessions = ["ERPCHAIN", "PRJOLD1"]
    study_new.save()
    return study_old, study_new, ena_study


@pytest.mark.django_db
def test_find_duplicated_studies_returns_matching_pair():
    study_old, study_new, _ = _create_study_pair()

    groups = find_duplicated_studies()

    assert len(groups) == 1
    assert [study.accession for study in groups[0]] == sorted(
        [study_old.accession, study_new.accession]
    )


@pytest.mark.django_db
def test_find_duplicated_studies_returns_overlap_pair():
    ena_study = ena.models.Study.objects.create(
        accession="PRJOVER1",
        additional_accessions=["ERPOVER1"],
        title="Overlap ENA",
    )
    study_old = Study.objects.create(ena_study=ena_study, title="Old")
    study_new = Study.objects.create(ena_study=ena_study, title="New")
    study_old.ena_accessions = ["PRJOVER1", "ERPOVER1"]
    study_new.ena_accessions = ["PRJOVER1"]
    study_old.save()
    study_new.save()

    groups = find_duplicated_studies()

    assert len(groups) == 1
    assert {study.id for study in groups[0]} == {study_old.id, study_new.id}


@pytest.mark.django_db
def test_deduplicate_studies_dry_run_leaves_data_unchanged(prefect_harness):
    study_old, study_new, _ = _create_study_pair()
    sample = Sample.objects.create(
        ena_sample=ena.models.Sample.objects.create(
            accession="SAMEA-DRY",
            study=study_new.ena_study,
        ),
        ena_study=study_new.ena_study,
        ena_accessions=["SAMEA-DRY"],
    )
    sample.studies.add(study_new)
    Run.objects.create(
        ena_accessions=["ERR-DRY"],
        study=study_new,
        ena_study=study_new.ena_study,
        sample=sample,
    )

    results = deduplicate_studies(dry_run=True)

    assert Study.objects.filter(id=study_old.id).exists()
    assert Study.objects.filter(id=study_new.id).exists()
    assert (
        Run.objects.get(ena_accessions__contains=["ERR-DRY"]).study_id == study_new.id
    )
    artifact = Artifact.get("study-deduplication-summary")
    assert artifact.type == "table"
    assert len(json.loads(artifact.data)) == len(results) == 1


@pytest.mark.django_db
def test_deduplicate_studies_rewires_relations(prefect_harness):
    study_old, study_new, ena_study = _create_study_pair()
    study_old.title = ""
    study_old.metadata = {"keep": "me"}
    study_old.results_dir = None
    study_old.external_results_dir = None
    study_old.features = {"has_prev6_analyses": False, "has_v6_analyses": False}
    study_old.save()

    study_new.title = "Preferred title"
    study_new.metadata = {"duplicate": "value"}
    study_new.results_dir = "/work/results"
    study_new.external_results_dir = "/work/external"
    study_new.features = {"has_prev6_analyses": True, "has_v6_analyses": False}
    study_new.save()

    ena_sample_old = ena.models.Sample.objects.create(
        accession="SAMEA1", study=ena_study
    )
    mg_sample = Sample.objects.create(
        ena_sample=ena_sample_old,
        ena_study=ena_study,
        ena_accessions=["SAMEA1"],
    )
    mg_sample.studies.add(study_new)

    run = Run.objects.create(
        ena_accessions=["ERR1"],
        study=study_new,
        ena_study=ena_study,
        sample=mg_sample,
    )
    assembly = Assembly.objects.create(
        ena_accessions=["ERZ1"],
        ena_study=ena_study,
        sample=mg_sample,
        reads_study=study_new,
        assembly_study=study_new,
    )
    analysis = Analysis.objects.create(
        study=study_new,
        sample=mg_sample,
        run=run,
        assembly=assembly,
        ena_study=ena_study,
    )
    AssemblyAnalysisBatch.objects.create(
        study=study_new,
        batch_type="assembly_analysis",
        workspace_dir="/tmp/study-batch",
        total_analyses=1,
    )
    publication = Publication.objects.create(
        pubmed_id=123456,
        title="Study publication",
        metadata={},
    )
    StudyPublication.objects.create(study=study_new, publication=publication)
    super_study = SuperStudy.objects.create(
        slug="merged-super-study", title="Super Study"
    )
    SuperStudyStudy.objects.create(study=study_new, super_study=super_study)

    deduplicate_studies(dry_run=False)

    study_old.refresh_from_db()
    mg_sample.refresh_from_db()
    run.refresh_from_db()
    assembly.refresh_from_db()
    analysis.refresh_from_db()

    assert not Study.objects.filter(id=study_new.id).exists()
    assert Study.objects.count() == 1
    assert mg_sample.studies.filter(id=study_old.id).exists()
    assert run.study_id == study_old.id
    assert assembly.reads_study_id == study_old.id
    assert assembly.assembly_study_id == study_old.id
    assert analysis.study_id == study_old.accession
    assert study_old.title == "Preferred title"
    assert study_old.metadata == {"duplicate": "value", "keep": "me"}
    assert study_old.results_dir == "/work/results"
    assert study_old.external_results_dir == "/work/external"
    assert study_old.features.has_prev6_analyses is True
    assert StudyPublication.objects.filter(
        study=study_old, publication=publication
    ).exists()
    assert SuperStudyStudy.objects.filter(
        study=study_old, super_study=super_study
    ).exists()


@pytest.mark.django_db
def test_deduplicate_studies_accession_filter_limits_groups(prefect_harness):
    target_old, target_new, _ = _create_study_pair()
    other_ena_study = ena.models.Study.objects.create(
        accession="PRJOTHER1",
        additional_accessions=["ERPOTHER1"],
    )
    other_old = Study.objects.create(ena_study=other_ena_study, title="Other old")
    other_new = Study.objects.create(ena_study=other_ena_study, title="Other new")
    other_old.ena_accessions = ["PRJOTHER1", "ERPOTHER1"]
    other_new.ena_accessions = ["PRJOTHER1", "ERPOTHER1"]
    other_old.save()
    other_new.save()

    deduplicate_studies(accessions=[target_old.first_accession], dry_run=False)

    assert Study.objects.filter(id=target_new.id).exists() is False
    assert Study.objects.filter(id=other_new.id).exists() is True
