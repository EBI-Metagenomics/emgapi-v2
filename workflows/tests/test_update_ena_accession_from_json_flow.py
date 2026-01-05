import json
from typing import Optional
import pytest

from analyses.models import Biome
from genomes.models import Genome, GenomeCatalogue
from workflows.flows.update_ena_accession_from_json_flow import (
    update_ena_accession_from_json_flow,
)
from workflows.prefect_utils.testing_utils import (
    run_flow_and_capture_logs,
    should_not_mock_httpx_requests_to_prefect_server,
)


def _make_basic_genome_env():
    biome = Biome.objects.create(biome_name="root")
    catalogue = GenomeCatalogue.objects.create(
        catalogue_id="cat-a",
        version="v1",
        name="Catalogue A",
        catalogue_biome_label="root",
        catalogue_type=GenomeCatalogue.PROK,
    )
    return biome, catalogue


def _create_genome(
    accession: str,
    biome: Biome,
    catalogue: GenomeCatalogue,
    ena_genome_accession: Optional[str] = None,
):
    return Genome.objects.create(
        accession=accession,
        biome=biome,
        length=100000,
        num_contigs=10,
        n_50=1000,
        gc_content=0.5,
        type="MAG",
        completeness=95.0,
        contamination=1.0,
        trnas=20.0,
        nc_rnas=10,
        num_proteins=1000,
        eggnog_coverage=80.0,
        ipr_coverage=70.0,
        taxon_lineage="Bacteria;Firmicutes",
        catalogue=catalogue,
        ena_genome_accession=ena_genome_accession,
    )


# @pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_update_ena_accession_from_json_flow_basic(prefect_harness, tmp_path):
    biome, catalogue = _make_basic_genome_env()

    # Create 6 genomes to exercise all branches
    g1 = _create_genome("MGYG000000001", biome, catalogue)  # update via ncbi_genome_accession
    g2 = _create_genome("MGYG000000002", biome, catalogue)  # update via genome_accession (fallback)
    g3 = _create_genome("MGYG000000003", biome, catalogue, ena_genome_accession="GCA_000000003.1")  # skip no change
    g4 = _create_genome("MGYG000000004", biome, catalogue)  # missing file
    g5 = _create_genome("MGYG000000005", biome, catalogue)  # invalid JSON
    g6 = _create_genome("MGYG000000006", biome, catalogue)  # empty accession value

    base = tmp_path

    def write_json(acc, payload):
        acc_dir = base / acc
        acc_dir.mkdir(parents=True, exist_ok=True)
        (acc_dir / f"{acc}.json").write_text(json.dumps(payload))

    # g1: has ncbi_genome_accession
    write_json(g1.accession, {"ncbi_genome_accession": "GCA_000000001.1"})
    # g2: fallback key genome_accession
    write_json(g2.accession, {"genome_accession": "GCA_000000002.2"})
    # g3: value equals existing -> skip
    write_json(g3.accession, {"ncbi_genome_accession": "GCA_000000003.1"})
    # g4: no file
    # g5: invalid JSON
    acc5_dir = base / g5.accession
    acc5_dir.mkdir(parents=True, exist_ok=True)
    (acc5_dir / f"{g5.accession}.json").write_text("{ this is not: valid json }")
    # g6: empty string value -> treated as missing key/empty
    write_json(g6.accession, {"ncbi_genome_accession": "  "})

    logged = run_flow_and_capture_logs(
        update_ena_accession_from_json_flow,
        str(base),
        2,  # read_chunk_size
        1,  # update_batch_size to force batching flush
        None,  # catalogue filter
    )

    summary = logged.result

    assert summary["total_genomes_seen"] == 6
    assert summary["total_genomes_with_file"] == 5
    assert summary["total_missing_file"] == 1
    assert summary["total_missing_key_or_parse_error"] == 2
    assert summary["total_marked_for_update"] == 2
    assert summary["total_updated"] == 2
    assert summary["total_skipped_no_change"] == 1

    # Verify DB updates
    g1.refresh_from_db(); g2.refresh_from_db(); g3.refresh_from_db(); g4.refresh_from_db(); g5.refresh_from_db(); g6.refresh_from_db()
    assert g1.ena_genome_accession == "GCA_000000001.1"
    assert g2.ena_genome_accession == "GCA_000000002.2"
    assert g3.ena_genome_accession == "GCA_000000003.1"  # unchanged
    assert g4.ena_genome_accession is None
    assert g5.ena_genome_accession is None
    assert g6.ena_genome_accession is None


@pytest.mark.httpx_mock(should_mock=should_not_mock_httpx_requests_to_prefect_server)
@pytest.mark.django_db(transaction=True)
def test_update_ena_accession_from_json_flow_catalogue_filter(prefect_harness, tmp_path):
    # Two catalogues
    biome = Biome.objects.create(biome_name="root")
    cat_a = GenomeCatalogue.objects.create(
        catalogue_id="cat-a",
        version="v1",
        name="Catalogue A",
        catalogue_biome_label="root",
        catalogue_type=GenomeCatalogue.PROK,
    )
    cat_b = GenomeCatalogue.objects.create(
        catalogue_id="cat-b",
        version="v2",
        name="Catalogue B",
        catalogue_biome_label="root",
        catalogue_type=GenomeCatalogue.PROK,
    )

    gA1 = _create_genome("MGYG000010001", biome, cat_a)  # should update
    gA2 = _create_genome("MGYG000010002", biome, cat_a)  # missing file, counted
    gB1 = _create_genome("MGYG000020001", biome, cat_b)  # should be ignored entirely

    base = tmp_path

    # Files for A1 and B1
    for g, val in [(gA1, "GCA_100000001.1"), (gB1, "GCA_200000001.1")]:
        d = base / g.accession
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{g.accession}.json").write_text(json.dumps({"ncbi_genome_accession": val}))

    logged = run_flow_and_capture_logs(
        update_ena_accession_from_json_flow,
        str(base),
        10,  # read_chunk_size
        10,  # update_batch_size
        cat_a.name,  # filter to Catalogue A
    )
    summary = logged.result

    # Only genomes from Catalogue A are seen
    assert summary["total_genomes_seen"] == 2
    assert summary["total_genomes_with_file"] == 1
    assert summary["total_missing_file"] == 1
    assert summary["total_missing_key_or_parse_error"] == 0
    assert summary["total_marked_for_update"] == 1
    assert summary["total_updated"] == 1
    assert summary["total_skipped_no_change"] == 0

    # A1 updated, A2 unchanged, B1 ignored
    gA1.refresh_from_db(); gA2.refresh_from_db(); gB1.refresh_from_db()
    assert gA1.ena_genome_accession == "GCA_100000001.1"
    assert gA2.ena_genome_accession is None
    assert gB1.ena_genome_accession is None
