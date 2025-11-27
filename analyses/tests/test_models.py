import csv
import os
import tempfile

import pytest
from django.core.management import call_command

from analyses.models import (
    Analysis,
    Assembler,
    Biome,
    ComputeResourceHeuristic,
    Run,
    Study,
)
from ena.models import Study as ENAStudy


def create_analysis(is_private=False):
    run = Run.objects.first()
    return Analysis.objects.create(
        study=run.study,
        run=run,
        ena_study=run.ena_study,
        sample=run.sample,
        is_private=is_private,
    )


@pytest.mark.django_db(transaction=True, reset_sequences=True)
def test_study():
    # Test accessioning
    ena_study = ENAStudy.objects.create(accession="PRJ1", title="Project 1")
    study = Study.objects.create(ena_study=ena_study, title="Project 1")
    assert study.accession == "MGYS00000001"
    study.inherit_accessions_from_related_ena_object("ena_study")
    assert "PRJ1" in study.ena_accessions
    study.ena_study.additional_accessions = ["ERP1"]
    study.ena_study.save()
    study.inherit_accessions_from_related_ena_object("ena_study")
    assert "PRJ1" in study.ena_accessions
    assert "ERP1" in study.ena_accessions
    assert study.first_accession == "ERP1"
    study.ena_accessions = ["PRJ1", "ERP1"]
    study.save()
    assert study.first_accession == "ERP1"


def test_biome_lineage_path_generator():
    assert Biome.lineage_to_path("root") == "root"
    assert Biome.lineage_to_path("Root") == "root"
    assert (
        Biome.lineage_to_path("root:Host-associated:Human:Digestive system")
        == "root.host_associated.human.digestive_system"
    )
    assert (
        Biome.lineage_to_path("root:Host-associated:Human:Digestive system (bowel)")
        == "root.host_associated.human.digestive_system_bowel"
    )


@pytest.mark.django_db(transaction=True)
def test_biome_hierarchy():
    root: Biome = Biome.objects.create(biome_name="root", path="root")
    engineered: Biome = Biome.objects.create(
        biome_name="Engineered", path="root.engineered"
    )
    wet_ferm: Biome = Biome.objects.create(
        biome_name="Wet fermentation", path="root.engineered.wet_fermentation"
    )
    cont_cult: Biome = Biome.objects.create(
        biome_name="Continuous culture",
        path="root.engineered.bioreactor.continuous_culture",
    )

    # only one root to hierarchy
    assert Biome.objects.roots().count() == 1

    # root has 4 descendants (includes self)
    root.refresh_from_db()
    assert root.descendants_count == 4

    # engineered has 3
    engineered.refresh_from_db()
    assert engineered.descendants_count == 3

    # wet fermentaiton has 1 (self)
    wet_ferm.refresh_from_db()
    assert wet_ferm.descendants_count == 1

    # pretty lineage is colon separated with capitals and spaces
    assert wet_ferm.pretty_lineage == "root:Engineered:Wet fermentation"

    # if a level of hierarchy is skipped (bioreactor) it is simply missing in pretty version (known limitation)
    cont_cult.refresh_from_db()
    assert cont_cult.pretty_lineage == "root:Engineered:Continuous culture"

    # ancestors are as expected (self, engineered, root)
    assert cont_cult.ancestors().count() == 3
    assert cont_cult.ancestors().filter(biome_name="root").exists()
    assert cont_cult.ancestors().filter(biome_name="Engineered").exists()
    assert cont_cult.ancestors().filter(biome_name="Continuous culture").exists()

    # (direct) children should be 1
    assert engineered.children().count() == 1
    # add another child
    Biome.objects.create(biome_name="Bioreactor", path="root.engineered.bioreactor")
    assert engineered.children().count() == 2


@pytest.mark.django_db(transaction=True)
def test_study_biome_lookups(top_level_biomes, raw_reads_mgnify_study):
    cont_cult: Biome = Biome.objects.create(
        biome_name="Continuous culture",
        path="root.engineered.bioreactor.continuous_culture",
    )
    raw_reads_mgnify_study.biome = cont_cult
    raw_reads_mgnify_study.save()

    # something like a lineage lookup:
    eng = Biome.objects.get(path=Biome.lineage_to_path("root:Engineered"))

    # find studies within lineage. this lookup is for studies with a biome whose path is IN the descendants of engineered
    # (slightly counterintuitive naming)
    eng_studies = Study.objects.filter(biome__path__descendants=eng.path)
    assert eng_studies.count() == 1
    assert eng_studies.first() == raw_reads_mgnify_study


@pytest.mark.django_db(transaction=True)
def test_compute_resource_heuristics(top_level_biomes, assemblers):
    # test CSV importer
    with tempfile.NamedTemporaryFile(mode="w", delete=False, newline="") as temp_csv:
        writer = csv.DictWriter(
            temp_csv, fieldnames=["lineage", "memory_gb", "assembler"]
        )
        writer.writeheader()
        writer.writerow(
            {
                "lineage": "root:Host-associated",
                "memory_gb": 100,
                "assembler": "metaspades",
            }
        )
        writer.writerow(
            {
                "lineage": "root:Host-associated:Human",
                "memory_gb": 50,
                "assembler": "metaspades",
            }
        )
        temp_csv_name = temp_csv.name

    try:
        call_command("import_assembler_memory_compute_heuristics", "-p", temp_csv_name)

        assert ComputeResourceHeuristic.objects.count() == 2

        # should select the closest ancestor biome of a deep biome
        deep_biome = Biome.objects.create(
            path=Biome.lineage_to_path("root:Host-associated:Human:Digestive system"),
            biome_name="Digestive system",
        )
        ram_for_deep_biome = (
            ComputeResourceHeuristic.objects.filter(
                process=ComputeResourceHeuristic.ProcessTypes.ASSEMBLY,
                assembler=assemblers.get(name=Assembler.METASPADES),
                biome__path__ancestors=deep_biome.path,
            )
            .reverse()
            .first()
        )
        assert ram_for_deep_biome is not None
        assert ram_for_deep_biome.memory_gb == 50

    finally:
        os.remove(temp_csv_name)


@pytest.mark.django_db(transaction=True)
def test_biome_importer(httpx_mock):
    httpx_mock.add_response(
        url="http://old.api/v1/biomes?page=1",
        json={
            "links": {
                "next": "http://old.api/v1/biomes?page=2",
            },
            "data": [{"id": "root", "attributes": {"biome-name": "Root"}}],
        },
    )
    httpx_mock.add_response(
        url="http://old.api/v1/biomes?page=2",
        json={
            "links": {
                "next": None,
            },
            "data": [{"id": "root:Deep", "attributes": {"biome-name": "Deep"}}],
        },
    )
    call_command("import_biomes_from_api_v1", "-u", "http://old.api/v1/biomes")
    assert Biome.objects.count() == 2
    assert Biome.objects.filter(path="root.deep").exists()
    assert Biome.objects.get(path="root.deep").biome_name == "Deep"
    assert Biome.objects.get(path="root.deep").pretty_lineage == "root:Deep"


@pytest.mark.django_db(transaction=True)
def test_analysis_inheritance(
    raw_read_run,
):
    for run in Run.objects.all():
        analysis = Analysis.objects.create(
            study=run.study, run=run, ena_study=run.ena_study, sample=run.sample
        )
        analysis.inherit_experiment_type()
        analysis.refresh_from_db()
        assert analysis.experiment_type != analysis.ExperimentTypes.UNKNOWN
        assert analysis.experiment_type == run.experiment_type


@pytest.mark.django_db(transaction=True)
def test_status_filtering(
    raw_read_run,
):
    run = Run.objects.first()
    analysis = Analysis.objects.create(
        study=run.study, run=run, ena_study=run.ena_study, sample=run.sample
    )
    assert analysis.AnalysisStates.ANALYSIS_COMPLETED.value in analysis.status
    assert not analysis.status[analysis.AnalysisStates.ANALYSIS_COMPLETED.value]

    assert run.study.analyses.count() == 1

    assert (
        run.study.analyses.filter(
            **{f"status__{analysis.AnalysisStates.ANALYSIS_COMPLETED.value}": False}
        ).count()
        == 1
    )
    assert (
        run.study.analyses.filter(
            **{f"status__{analysis.AnalysisStates.ANALYSIS_COMPLETED.value}": True}
        ).count()
        == 0
    )

    assert (
        run.study.analyses.filter_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_COMPLETED]
        ).count()
        == 0
    )

    # should include a missing key if not strict
    assert run.study.analyses.filter_by_statuses(["non_existent_key"]).count() == 0
    assert (
        run.study.analyses.filter_by_statuses(
            ["non_existent_key"], strict=False
        ).count()
        == 1
    )

    analysis.status[analysis.AnalysisStates.ANALYSIS_COMPLETED] = True
    analysis.save()
    assert (
        run.study.analyses.filter_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_COMPLETED]
        ).count()
        == 1
    )

    assert (
        run.study.analyses.filter_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_COMPLETED, "non_existent_key"]
        ).count()
        == 0
    )
    assert (
        run.study.analyses.filter_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_COMPLETED, "non_existent_key"],
            strict=False,
        ).count()
        == 1
    )

    analysis.status["an_extra_key"] = True
    analysis.save()
    assert (
        run.study.analyses.filter_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_COMPLETED, "an_extra_key"]
        ).count()
        == 1
    )

    # EXCLUSIONS
    assert run.study.analyses.count() == 1
    assert (
        run.study.analyses.exclude_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_COMPLETED]
        ).count()
        == 0
    )
    assert (
        run.study.analyses.exclude_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_FAILED]
        ).count()
        == 1
    )
    assert (
        run.study.analyses.exclude_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_FAILED, "an_extra_key"]
        ).count()
        == 0
    )

    assert (
        run.study.analyses.exclude_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_FAILED, "non_existent_key"]
        ).count()
        == 1
    )
    assert (
        run.study.analyses.exclude_by_statuses(
            [analysis.AnalysisStates.ANALYSIS_FAILED, "non_existent_key"], strict=False
        ).count()
        == 0
    )

    # FILTERING/EXCLUDING WITH CHAINING
    assert run.study.analyses.count() == 1
    assert (
        run.study.analyses.filter(pipeline_version=Analysis.PipelineVersions.v6).count()
        == 1
    )
    assert (
        run.study.analyses.filter(pipeline_version=Analysis.PipelineVersions.v5).count()
        == 0
    )

    analysis.status[analysis.AnalysisStates.ANALYSIS_COMPLETED] = True
    analysis.save()

    assert (
        run.study.analyses.filter(pipeline_version=Analysis.PipelineVersions.v6)
        .filter_by_statuses([analysis.AnalysisStates.ANALYSIS_COMPLETED])
        .count()
        == 1
    )
    assert (
        run.study.analyses.filter(pipeline_version=Analysis.PipelineVersions.v6)
        .exclude_by_statuses([analysis.AnalysisStates.ANALYSIS_COMPLETED])
        .count()
        == 0
    )

    analysis.status[analysis.AnalysisStates.ANALYSIS_COMPLETED] = False
    analysis.save()

    assert (
        run.study.analyses.filter(pipeline_version=Analysis.PipelineVersions.v6)
        .filter_by_statuses([analysis.AnalysisStates.ANALYSIS_COMPLETED])
        .count()
        == 0
    )
    assert (
        run.study.analyses.filter(pipeline_version=Analysis.PipelineVersions.v6)
        .exclude_by_statuses([analysis.AnalysisStates.ANALYSIS_COMPLETED])
        .count()
        == 1
    )


@pytest.mark.django_db(transaction=True)
def test_update_or_create_by_accession(raw_reads_mgnify_study):
    ena_study = raw_reads_mgnify_study.ena_study
    # start with just one accession in db
    assert ena_study.additional_accessions == []
    assert raw_reads_mgnify_study.ena_accessions == ["PRJNA398089"]

    # updating nothing should be equivalent to get by single accession
    assert Study.objects.update_or_create_by_accession(
        known_accessions=["PRJNA398089"],
    ) == (raw_reads_mgnify_study, False)

    # updating nothing but using additional known accession should set the additional accession in db
    study, created = Study.objects.update_or_create_by_accession(
        known_accessions=["PRJNA398089", "ERP1"],
    )
    assert not created
    assert set(study.ena_accessions) == {"PRJNA398089", "ERP1"}
    raw_reads_mgnify_study.refresh_from_db()
    assert set(raw_reads_mgnify_study.ena_accessions) == {"PRJNA398089", "ERP1"}

    # secondary accession should now be equivalent to first - usable for get/update etc and not create nor overwrite
    study, created = Study.objects.update_or_create_by_accession(
        known_accessions=["ERP1"],
    )
    assert not created
    assert set(study.ena_accessions) == {"PRJNA398089", "ERP1"}
    raw_reads_mgnify_study.refresh_from_db()
    assert set(raw_reads_mgnify_study.ena_accessions) == {"PRJNA398089", "ERP1"}

    # update other fields should work too – create-only defaults should not be set though
    study, created = Study.objects.update_or_create_by_accession(
        known_accessions=["ERP1"],
        defaults={"results_dir": "/has/been/set"},
        create_defaults={"title": "Unchanged"},
    )
    assert not created
    assert set(study.ena_accessions) == {"PRJNA398089", "ERP1"}
    assert not study.title == "Unchanged"
    assert study.results_dir == "/has/been/set"

    # create defaults setting should work for new objects
    study, created = Study.objects.update_or_create_by_accession(
        known_accessions=["ERP2", "PRJ2"],
        defaults={"results_dir": "/has/been/set"},
        create_defaults={"title": "Set at create"},
        include_update_defaults_in_create_defaults=False,
    )
    assert created
    assert set(study.ena_accessions) == {"PRJ2", "ERP2"}
    assert study.title == "Set at create"
    assert not study.results_dir  # not set because update-only, not create

    # unspecified create defaults should then use update defaults, like django default
    study, created = Study.objects.update_or_create_by_accession(
        known_accessions=["ERP3", "PRJ3"],
        defaults={"results_dir": "/has/been/set", "title": "Set always"},
    )
    assert created
    assert set(study.ena_accessions) == {"PRJ3", "ERP3"}
    assert study.title == "Set always"
    assert study.results_dir == "/has/been/set"

    # create defaults should include update defaults if requested (default, unlike django)
    study, created = Study.objects.update_or_create_by_accession(
        known_accessions=["ERP4", "PRJ4"],
        defaults={"results_dir": "/has/been/set"},
        create_defaults={"title": "Set at create"},
        include_update_defaults_in_create_defaults=True,
    )
    assert created
    assert set(study.ena_accessions) == {"PRJ4", "ERP4"}
    assert study.title == "Set at create"
    assert study.results_dir == "/has/been/set"


@pytest.mark.django_db
def test_inferred_metadata_mixin(raw_read_run):
    run = Run.objects.first()
    run.metadata = {}
    run.save()

    assert run.metadata == {}
    assert run.metadata_preferring_inferred == {}

    run.metadata = {"kessel_run": 20, "falcon": "millenium"}
    run.save()
    assert run.metadata_preferring_inferred["kessel_run"] == 20
    assert run.metadata_preferring_inferred["falcon"] == "millenium"
    run.metadata["inferred_kessel_run"] = "20 parsecs"
    run.save()
    run.refresh_from_db()
    # some process has "inferred" an override value for kessel_run. that should be used instead of the original value,
    # when metadata is fetched via metadata_preferring_inferred
    assert run.metadata_preferring_inferred["kessel_run"] == "20 parsecs"
    assert run.metadata["kessel_run"] == 20
    assert run.metadata_preferring_inferred["falcon"] == "millenium"
