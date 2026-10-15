from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analyses.models import Analysis, Biome
from proteins.dataset import layout, metadata
from proteins.dataset.update import ASSEMBLY_PROTEINS
from proteins.tests.conftest import make_analysis, make_study
from workflows.flows.proteins import export_assembly_analyses as export_flow

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
        ASSEMBLY_PROTEINS,
    )


@pytest.fixture
def engineered():
    Biome.objects.create(biome_name="root", path="root")
    return Biome.objects.create(biome_name="Engineered", path="root.engineered")


def test_one_row_per_record_with_the_study_and_biome_of_its_mgya(engineered):
    a, b = make_study("PRJ1"), make_study("PRJ2")
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
            "biome_lineage": None,
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
@pytest.mark.parametrize(
    "on_assembly, on_study",
    [(False, False), (True, False), (False, True), (True, True)],
)
def test_a_flag_is_set_when_the_assembly_or_its_study_has_it(
    flag, on_assembly, on_study
):
    study = make_study("PRJ1")
    analysis = make_analysis(study, "ERZ1")
    for model, value in ((analysis.assembly, on_assembly), (study, on_study)):
        type(model).objects.filter(pk=model.pk).update(**{flag: value})
    expected = on_assembly or on_study

    (row,) = metadata.build_snapshot(
        registry((1, analysis, "assembly_analysis"))
    ).to_pylist()
    assert row[flag] is expected
    (row,) = metadata.export_assembly_analyses().to_pylist()
    assert row[flag] is expected


@pytest.mark.usefixtures("prefect_harness")
def test_the_export_has_the_lowest_mgya_of_each_erz_and_version(engineered, tmp_path):
    a, b = make_study("PRJ1"), make_study("PRJ2")
    v5 = make_analysis(a, "ERZ1", "V5")
    make_analysis(b, "ERZ1", "V5")
    v4_1 = make_analysis(a, "ERZ1", "V4.1")
    v6 = make_analysis(b, "ERZ2", state=Analysis.AnalysisStates.ANALYSIS_STARTED)
    v6.assembly.ena_accessions = ["ERZ2", "ERZ3"]
    v6.assembly.save()
    v6.sample.biome = engineered
    v6.sample.save()

    out = tmp_path / "export" / "assembly_analyses.parquet"
    assert export_flow.export_assembly_analyses(str(out)) == 4
    assert list(out.parent.iterdir()) == [out]
    table = pq.read_table(out)
    assert table.schema == metadata.ASSEMBLY_ANALYSES
    row = dict(is_private=False, is_suppressed=False)
    assert table.to_pylist() == [
        dict(
            assembly_accession="ERZ1",
            pipeline_version="5.0",
            analysis_accession=v5.accession,
            study_accession=a.accession,
            biome_lineage=None,
            **row,
        ),
        dict(
            assembly_accession="ERZ1",
            pipeline_version="4.1",
            analysis_accession=v4_1.accession,
            study_accession=a.accession,
            biome_lineage=None,
            **row,
        ),
        *(
            dict(
                assembly_accession=erz,
                pipeline_version="6.0",
                analysis_accession=v6.accession,
                study_accession=b.accession,
                biome_lineage="root:Engineered",
                **row,
            )
            for erz in ("ERZ2", "ERZ3")
        ),
    ]
