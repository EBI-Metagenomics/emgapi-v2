import csv
import logging

from django.core.management.base import BaseCommand

from analyses.models import Assembler, Biome, ComputeResourceHeuristic


class Command(BaseCommand):
    help = "Imports biomes from API v1."

    def add_arguments(self, parser):
        parser.add_argument(
            "-p",
            "--path",
            type=str,
            help="Path to a csv file with lineage, assembler, memory_gb columns.",
            required=True,
        )

    def handle(self, *args, **options):
        with open(options["path"]) as f:
            csv_reader = csv.DictReader(f)
            for heuristic in csv_reader:
                assembler_name = heuristic["assembler"]
                assembler_objs = Assembler.objects.filter(name=assembler_name)
                biome_lineage = heuristic["lineage"]
                biome_objs = Biome.objects.filter(
                    path=Biome.lineage_to_path(biome_lineage)
                )
                if not biome_objs.exists():
                    logging.warning(f"Biome <<{biome_lineage}>> not found in db!")
                    continue
                for assembler_obj in assembler_objs:
                    _, created = ComputeResourceHeuristic.objects.get_or_create(
                        assembler=assembler_obj,
                        biome=biome_objs.first(),
                        process=ComputeResourceHeuristic.ProcessTypes.ASSEMBLY,
                        defaults={"memory_gb": int(float(heuristic["memory_gb"]))},
                    )
                    if not created:
                        logging.warning(
                            f"Heuristic for {assembler_name}, biome <<{biome_lineage}>> already existed. Not overwriting."
                        )
