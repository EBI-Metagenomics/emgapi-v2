import pyarrow as pa
import pytest

import analyses.models as mg_models
from proteins import dims
from proteins.dims import Source, resolve, snapshot
from proteins.tier2 import SCHEMAS


def analyse(assembly, study):
    return mg_models.Analysis.objects.create(
        study=study,
        sample=assembly.sample,
        assembly=assembly,
        ena_study=study.ena_study,
        pipeline_version=mg_models.Analysis.PipelineVersions.v6,
    )


@pytest.mark.django_db
@pytest.mark.parametrize("chunk", [dims.CHUNK, 1])
def test_resolve_takes_the_study_and_biome_of_the_newest_analysis(
    monkeypatch, mgnify_assemblies, raw_reads_mgnify_study, webin_private_study, chunk
):
    monkeypatch.setattr(dims, "CHUNK", chunk)
    raw_reads_mgnify_study.biome = mg_models.Biome.objects.get(
        path="root.host_associated.human"
    )
    raw_reads_mgnify_study.save()
    webin_private_study.biome = None
    webin_private_study.save()
    two_accessions, moved, never_analysed = mgnify_assemblies[:3]
    two_accessions.ena_accessions = ["ERZ1", "GCA_1"]
    two_accessions.is_suppressed = True
    two_accessions.save()
    analyse(two_accessions, raw_reads_mgnify_study)
    analyse(moved, raw_reads_mgnify_study)
    analyse(moved, webin_private_study)

    known = Source(
        "ERZ1",
        raw_reads_mgnify_study.accession,
        "root:Host-Associated:Human",
        False,
        True,
        False,
        False,
    )
    assert resolve(
        [
            "ERZ1",
            moved.first_accession,
            never_analysed.first_accession,
            "ERZ_UNKNOWN",
            "GCA_1",
        ]
    ) == [
        known,
        Source(
            moved.first_accession,
            webin_private_study.accession,
            None,
            False,
            False,
            True,
            False,
        ),
        known._replace(assembly_accession="GCA_1"),
    ]


@pytest.mark.django_db(databases=["default", "proteindb"], transaction=True)
def test_a_study_without_a_biome_registers_none(connect_as, emptied_tier1):
    conn = connect_as("proteindb_load")
    tables, unresolved = snapshot(
        conn,
        pa.table({"id": [1], "accession": ["ERZ1"], "pipeline_version": ["6.0"]}),
        SCHEMAS["dims/gene_caller"].empty_table(),
        lambda accessions: [Source("ERZ1", "MGYS1", None, False, False, False, False)],
    )
    assert tables["assembly"].to_pylist()[0]["biome_id"] is None
    assert tables["assembly"].to_pylist()[0]["study_id"] is not None
    assert tables["biome"].num_rows == 0
    assert unresolved == 0
