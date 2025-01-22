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


@pytest.mark.django_db(transaction=True, reset_sequences=True)
def test_study():
    # Test accessioning
    ena_study = ENAStudy.objects.create(accession="PRJ1", title="Project 1")
    study = Study.objects.create(ena_study=ena_study, title="Project 1")
    assert study.accession == "MGYS00000001"


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
