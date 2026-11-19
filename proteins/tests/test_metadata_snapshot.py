from unittest.mock import patch

import pyarrow as pa
import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext

from analyses.models import Analysis, Biome
from proteins.dataset import parquet_files
from proteins.dataset.export import metadata_snapshot
from proteins.tests.conftest import make_analysis, make_study, write_v1_analyses

pytestmark = pytest.mark.django_db


def registry(*records):
    """The assembly_proteins and assembly rows, from records (id, analysis, source). An ERZn's assembly id is n."""
    erzs = {analysis.assembly.first_accession for _, analysis, _ in records}
    return pa.Table.from_pylist(
        [
            {
                "id": id,
                "assembly_id": int(analysis.assembly.first_accession[3:]),
                "source": source,
                "pipeline_version": analysis.pipeline_major_minor,
                "analysis_accession": analysis.accession,
            }
            for id, analysis, source in records
        ],
        parquet_files.SCHEMAS["metadata/assembly_proteins_registry"],
    ), pa.Table.from_pylist(
        [{"id": int(erz[3:]), "accession": erz} for erz in erzs],
        parquet_files.SCHEMAS["metadata/assemblies"],
    )


@pytest.fixture
def engineered():
    Biome.objects.create(biome_name="root", path="root")
    return Biome.objects.create(biome_name="Engineered", path="root.engineered")


def test_one_row_per_record_with_the_study_and_biome_of_its_mgya(engineered, tmp_path):
    host = Biome.objects.create(biome_name="Host-associated", path="root.host")
    a, b = make_study("PRJ1"), make_study("PRJ2")
    # The sample's biome wins over the study's, which is taken when the sample has none.
    for study in a, b:
        study.biome = host
        study.save()
    # The ERP, wherever it is among the study's ENA accessions.
    a.ena_accessions = ["PRJEB1", "ERP000001"]
    a.save()
    v6 = make_analysis(a, "ERZ1")
    v6.sample.biome = engineered
    v6.sample.save()
    v6_1 = make_analysis(a, "ERZ1", version="V6.1")
    other = make_analysis(b, "ERZ2")

    with (
        patch.object(metadata_snapshot, "CHUNK", 2),
        patch.object(
            metadata_snapshot.Analysis.objects, "filter", wraps=Analysis.objects.filter
        ) as chunks,
    ):
        snapshot = metadata_snapshot.build_snapshot(
            *registry(
                (3, other, "assembly_analysis"),
                (1, v6, "assembly_analysis"),
                (2, v6_1, "gene_calling"),
            ),
            tmp_path,
        )
    assert chunks.call_count == 2
    assert snapshot.schema == parquet_files.SCHEMAS["metadata/assembly_proteins"]
    assert snapshot.to_pylist() == [
        {
            "assembly_proteins_id": 3,
            "assembly_id": 2,
            "assembly_accession": "ERZ2",
            "source": "assembly_analysis",
            "pipeline_version": "6.0",
            "analysis_accession": other.accession,
            "study_accession": b.accession,
            "ena_study_accession": None,
            "biome_lineage": "root:Host-associated",
            "is_private": False,
            "is_suppressed": False,
        },
        {
            "assembly_proteins_id": 1,
            "assembly_id": 1,
            "assembly_accession": "ERZ1",
            "source": "assembly_analysis",
            "pipeline_version": "6.0",
            "analysis_accession": v6.accession,
            "study_accession": a.accession,
            "ena_study_accession": "ERP000001",
            "biome_lineage": "root:Engineered",
            "is_private": False,
            "is_suppressed": False,
        },
        {
            "assembly_proteins_id": 2,
            "assembly_id": 1,
            "assembly_accession": "ERZ1",
            "source": "gene_calling",
            "pipeline_version": "6.1",
            "analysis_accession": v6_1.accession,
            "study_accession": a.accession,
            "ena_study_accession": "ERP000001",
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
    flag, on_analysis, on_assembly, on_study, tmp_path
):
    study = make_study("PRJ1")
    analysis = make_analysis(study, "ERZ1")
    for model, value in (
        (analysis, on_analysis),
        (analysis.assembly, on_assembly),
        (study, on_study),
    ):
        type(model).objects.filter(pk=model.pk).update(**{flag: value})

    (row,) = metadata_snapshot.build_snapshot(
        *registry((1, analysis, "assembly_analysis")), tmp_path
    ).to_pylist()
    assert row[flag] is (on_analysis or on_assembly or on_study)


def test_an_mgya_not_in_emgapi_takes_its_metadata_from_the_copy_of_v1(tmp_path):
    analysis = make_analysis(make_study("PRJ1"), "ERZ1")
    from_v1 = make_analysis(make_study("PRJ2"), "ERZ2", "V5")
    # Neither in emgapi-v2 nor in v1.
    gone = make_analysis(make_study("PRJ3"), "ERZ3", "V5")
    write_v1_analyses(
        tmp_path, analysis.accession, from_v1.accession, is_suppressed=True
    )
    records = registry(
        (1, analysis, "assembly_analysis"),
        (2, from_v1, "assembly_analysis"),
        (3, gone, "assembly_analysis"),
    )
    from_v1.delete()
    gone.delete()

    rows = metadata_snapshot.build_snapshot(*records, tmp_path).to_pylist()
    # emgapi-v2 wins over v1.
    assert rows[0]["analysis_accession"] == analysis.accession
    assert rows[0]["is_suppressed"] is False
    assert rows[1:] == [
        {
            "assembly_proteins_id": 2,
            "assembly_id": 2,
            "assembly_accession": "ERZ2",
            "source": "assembly_analysis",
            "pipeline_version": "5.0",
            "analysis_accession": from_v1.accession,
            "study_accession": "MGYS00000001",
            "ena_study_accession": "ERP1",
            "biome_lineage": "root:Engineered",
            "is_private": False,
            "is_suppressed": True,
        }
    ]


def test_the_biome_lineages_take_the_same_queries_for_any_number_of_biomes(
    engineered, tmp_path, django_assert_num_queries
):
    host = Biome.objects.create(biome_name="Host-associated", path="root.host")
    human = Biome.objects.create(biome_name="Human", path="root.host.human")
    study = make_study("PRJ1")
    analyses = []
    for erz, biome in ("ERZ1", engineered), ("ERZ2", host), ("ERZ3", human):
        analysis = make_analysis(study, erz)
        analysis.sample.biome = biome
        analysis.sample.save()
        analyses.append(analysis)

    with CaptureQueriesContext(connection) as one_biome:
        metadata_snapshot.find_analyses([analyses[0].accession], tmp_path)
    with django_assert_num_queries(len(one_biome)):
        known = metadata_snapshot.find_analyses(
            [analysis.accession for analysis in analyses], tmp_path
        )

    assert {mgya: row["biome_lineage"] for mgya, row in known.items()} == {
        analysis.accession: analysis.sample.biome.pretty_lineage
        for analysis in analyses
    }
