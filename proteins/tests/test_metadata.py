from unittest.mock import patch

import pyarrow as pa
import pytest

from analyses.models import Analysis, Biome
from proteins.dataset import layout, metadata
from proteins.tests.conftest import make_analysis, make_study

pytestmark = pytest.mark.django_db


def registry(*records):
    """assembly_proteins rows, each (id, analysis, source)."""
    return pa.Table.from_pylist(
        [
            {
                "id": id,
                "assembly_accession": analysis.assembly.first_accession,
                "source": source,
                "pipeline_version": analysis.pipeline_major_minor,
                "analysis_accession": analysis.accession,
            }
            for id, analysis, source in records
        ],
        layout.SCHEMAS["metadata/assembly_proteins_registry"],
    )


@pytest.fixture
def engineered():
    Biome.objects.create(biome_name="root", path="root")
    return Biome.objects.create(biome_name="Engineered", path="root.engineered")


def test_one_row_per_record_with_the_study_and_biome_of_its_mgya(engineered):
    host = Biome.objects.create(biome_name="Host-associated", path="root.host")
    a, b = make_study("PRJ1"), make_study("PRJ2")
    # The sample's biome wins over the study's, which is taken when the sample has none.
    for study in a, b:
        study.biome = host
        study.save()
    v6 = make_analysis(a, "ERZ1")
    v6.sample.biome = engineered
    v6.sample.save()
    v6_1 = make_analysis(a, "ERZ1", version="V6.1")
    other = make_analysis(b, "ERZ2")

    with (
        patch.object(metadata, "CHUNK", 2),
        patch.object(
            metadata.Analysis.objects, "filter", wraps=Analysis.objects.filter
        ) as chunks,
    ):
        snapshot = metadata.build_snapshot(
            registry(
                (3, other, "assembly_analysis"),
                (1, v6, "assembly_analysis"),
                (2, v6_1, "gene_calling"),
            )
        )
    assert chunks.call_count == 2
    assert snapshot.schema == layout.SCHEMAS["metadata/assembly_proteins"]
    assert snapshot.to_pylist() == [
        {
            "assembly_proteins_id": 3,
            "assembly_accession": "ERZ2",
            "source": "assembly_analysis",
            "pipeline_version": "6.0",
            "analysis_accession": other.accession,
            "study_accession": b.accession,
            "biome_lineage": "root:Host-associated",
            "is_private": False,
            "is_suppressed": False,
        },
        {
            "assembly_proteins_id": 1,
            "assembly_accession": "ERZ1",
            "source": "assembly_analysis",
            "pipeline_version": "6.0",
            "analysis_accession": v6.accession,
            "study_accession": a.accession,
            "biome_lineage": "root:Engineered",
            "is_private": False,
            "is_suppressed": False,
        },
        {
            "assembly_proteins_id": 2,
            "assembly_accession": "ERZ1",
            "source": "gene_calling",
            "pipeline_version": "6.1",
            "analysis_accession": v6_1.accession,
            "study_accession": a.accession,
            "biome_lineage": "root:Engineered",
            "is_private": False,
            "is_suppressed": False,
        },
    ]


@pytest.mark.parametrize("flag", ["is_private", "is_suppressed"])
@pytest.mark.parametrize("on_analysis", [False, True])
@pytest.mark.parametrize("on_assembly", [False, True])
@pytest.mark.parametrize("on_study", [False, True])
def test_a_flag_is_set_when_the_analysis_its_assembly_or_its_study_has_it(
    flag, on_analysis, on_assembly, on_study
):
    study = make_study("PRJ1")
    analysis = make_analysis(study, "ERZ1")
    for model, value in (
        (analysis, on_analysis),
        (analysis.assembly, on_assembly),
        (study, on_study),
    ):
        type(model).objects.filter(pk=model.pk).update(**{flag: value})

    (row,) = metadata.build_snapshot(
        registry((1, analysis, "assembly_analysis"))
    ).to_pylist()
    assert row[flag] is (on_analysis or on_assembly or on_study)
