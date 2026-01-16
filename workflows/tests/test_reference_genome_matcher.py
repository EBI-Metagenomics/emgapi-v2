from workflows.ena_utils.host_metadata_to_reference_genome import (
    get_reference_genome_for_host,
)


def test_get_reference_genome_for_host():
    assert get_reference_genome_for_host("cow").reference_genome.startswith("cow")
    assert get_reference_genome_for_host("bovine").reference_genome.startswith("cow")
    assert get_reference_genome_for_host("barley").reference_genome.startswith("barley")
    assert get_reference_genome_for_host("112509").reference_genome.startswith("barley")
    assert get_reference_genome_for_host("Hordeum vulgare").reference_genome.startswith(
        "barley"
    )
    assert get_reference_genome_for_host(
        "Hordeum vulgare cultivar mandalor"
    ).reference_genome.startswith("barley")
