import logging

from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand
from sqlalchemy import select

import ena.models
from analyses.models import Biome, Study
from workflows.data_io_utils.legacy_emg_dbs import LegacyStudy, legacy_emg_db_session


class Command(BaseCommand):
    help = "Imports studies from the legacy DB, without any contents - study metadata only."

    def add_arguments(self, parser):
        parser.add_argument(
            "-a",
            "--mgys_after",
            type=int,
            default=0,
            help="Only imports studies with an MGYS accession greater than this number.",
        )
        parser.add_argument(
            "-n",
            "--dry_run",
            action="store_true",
            help="If dry_run, importable data will be shown but not stored.",
        )

    def _make_study(self, legacy_study: LegacyStudy):
        legacy_biome = legacy_study.biome

        ena_study, created = ena.models.Study.objects.get_or_create(
            accession=legacy_study.ext_study_id,
            defaults={
                "additional_accessions": [legacy_study.project_id],
                "title": legacy_study.study_name,
            },
        )
        if created:
            logging.info(f"Created new ENA study object {ena_study}")

        biome = Biome.objects.get(
            path=Biome.lineage_to_path(legacy_biome.lineage),
        )

        mg_study = Study.objects.create(
            id=legacy_study.id,
            ena_study=ena_study,
            title=legacy_study.study_name,
            ena_accessions=[legacy_study.ext_study_id, legacy_study.project_id],
            biome=biome,
            is_private=legacy_study.is_private,
            is_suppressed=legacy_study.is_suppressed,
            has_legacy_data=True,
        )
        if created:
            logging.info(f"Created new study object {mg_study}")
        return mg_study

    def handle(self, *args, **options):
        dry_run = options.get("dry_run")
        after = options.get("mgys_after")

        if after:
            logging.warning(f"Only importing studies with MGYS > {after}")

        with legacy_emg_db_session() as session:
            studies_select_stmt = select(LegacyStudy).where(LegacyStudy.id > after)

            count = 0
            for legacy_study in session.execute(
                studies_select_stmt
            ).scalars():  # type: LegacyStudy
                print("THE STUDY")
                print(legacy_study)
                if Study.objects.filter(id=legacy_study.id).exists():
                    raise ValidationError(
                        f"Study {legacy_study.id} already exists in the non-legacy DB."
                    )

                if dry_run:
                    logging.info(
                        f"Would make study for MGYS {legacy_study.id} / {legacy_study.ext_study_id}"
                    )
                else:
                    self._make_study(legacy_study)
                count += 1

        logging.info(f"{'Would have' if dry_run else ''} Imported {count} studies")
