from emgapiv2.biome_lineage_utils import normalize_lineage


def test_normalize_biome_lineage():
    assert normalize_lineage(" root: Host-associated:: Human: ") == (
        "root:Host-associated:Human"
    )
