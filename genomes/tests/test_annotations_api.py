import pytest
from ninja.testing import TestClient

from emgapiv2.api import api
from kvstore.models import KeyValueStore
from kvstore.schemas import CogCategories


@pytest.mark.django_db
class TestGenomeAnnotationsAPI:
    def test_get_genome_annotations_augmented(self, genomes):
        client = TestClient(api)

        # KV Store entry
        cog_cats = CogCategories(
            root={
                "C": "Energy production and conversion",
                "J": "Translation, ribosomal structure and biogenesis",
            }
        )
        KeyValueStore.set_model("cog_category_description_map", cog_cats)

        # Use a genome from the fixture and give it some annotations
        genome = genomes[0]

        # API call
        url = f"/genomes/{genome.accession}/annotations"
        response = client.get(url)

        assert response.status_code == 200
        data = response.json()

        cog_data = data["annotations"]["cog_categories"]
        # Convert to dict for easier check
        cog_dict = {item["name"]: item for item in cog_data}

        assert cog_dict["C"]["description"] == "Energy production and conversion"
        assert (
            cog_dict["J"]["description"]
            == "Translation, ribosomal structure and biogenesis"
        )
        assert cog_dict["X"]["description"] == ""
