import httpx
from django.core.management.base import BaseCommand

from analyses.models import Biome


class Command(BaseCommand):
    help = "Imports biomes from API v1."

    def add_arguments(self, parser):
        parser.add_argument(
            "-u",
            "--url",
            type=str,
            help="URL endpoint for the API V1 biomes list",
            required=True,
        )

    def handle(self, *args, **options):
        endpoint = options.get("url")

        next_page = f"{endpoint}?page=1"

        Biome.objects.get_or_create(biome_name="root", path="root")

        while next_page:
            response = httpx.get(next_page)
            for biome in response.json()["data"]:
                path = Biome.lineage_to_path(biome["id"])
                biome_name = biome["attributes"]["biome-name"]
                print(f"Will create biome with path {path}")
                Biome.objects.get_or_create(
                    path=path, defaults={"biome_name": biome_name}
                )
            next_page = response.json()["links"].get("next")
