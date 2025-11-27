import logging

from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand
from sqlalchemy import select

from analyses.models import Study, SuperStudy, SuperStudyStudy
from workflows.data_io_utils.legacy_emg_dbs import (
    LegacySuperStudy,
    LegacySuperStudyStudy,
    legacy_emg_db_session,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Imports super studies and their associated studies from the legacy DB."

    def add_arguments(self, parser):
        parser.add_argument(
            "-n",
            "--dry_run",
            action="store_true",
            help="If dry_run, importable data will be shown but not stored.",
        )

    def _make_super_study(self, legacy_super_study: LegacySuperStudy, dry_run: bool):
        if SuperStudy.objects.filter(slug=legacy_super_study.url_slug).exists():
            raise ValidationError(
                f"Super Study with slug {legacy_super_study.url_slug} already exists in the non-legacy DB."
            )

        if dry_run:
            logger.info(
                f"Would make super study for ID {legacy_super_study.study_id} / {legacy_super_study.title}"
            )
            return None

        super_study = SuperStudy.objects.create(
            slug=legacy_super_study.url_slug,
            title=legacy_super_study.title,
            description=legacy_super_study.description or "",
            # Logo will be handled separately if needed
        )
        logger.info(f"Created new super study object {super_study}")
        return super_study

    def _associate_studies(
        self,
        legacy_super_study: LegacySuperStudy,
        super_study: SuperStudy,
        session,
        dry_run: bool,
    ):
        # Get all associated studies for this super study
        studies_select_stmt = select(LegacySuperStudyStudy).where(
            LegacySuperStudyStudy.super_study_id == legacy_super_study.study_id
        )

        count = 0
        for legacy_super_study_study in session.execute(
            studies_select_stmt
        ).scalars():  # type: LegacySuperStudyStudy
            try:
                study = Study.objects.get(id=legacy_super_study_study.study_id)
            except Study.DoesNotExist:
                logger.warning(
                    f"Study with ID {legacy_super_study_study.study_id} does not exist, skipping association"
                )
                continue

            if dry_run:
                logger.info(
                    f"Would associate study {study.accession} with super study {legacy_super_study.title} "
                    f"(is_flagship: {legacy_super_study_study.is_flagship})"
                )
            else:
                SuperStudyStudy.objects.create(
                    study=study,
                    super_study=super_study,
                    is_flagship=legacy_super_study_study.is_flagship,
                )
                logger.info(
                    f"Associated study {study.accession} with super study {super_study.slug}"
                )
            count += 1

        return count

    def handle(self, *args, **options):
        dry_run = options.get("dry_run")
        with legacy_emg_db_session() as session:
            super_studies_select_stmt = select(LegacySuperStudy)

            super_study_count = 0
            study_association_count = 0

            for legacy_super_study in session.execute(
                super_studies_select_stmt
            ).scalars():  # type: LegacySuperStudy
                super_study = self._make_super_study(legacy_super_study, dry_run)
                super_study_count += 1

                if dry_run:
                    # In dry run, we still want to log what would happen
                    study_association_count += self._associate_studies(
                        legacy_super_study, None, session, dry_run
                    )
                elif super_study:
                    study_association_count += self._associate_studies(
                        legacy_super_study, super_study, session, dry_run
                    )

        logger.info(
            f"{'Would have' if dry_run else ''} Imported {super_study_count} super studies "
            f"with {study_association_count} study associations"
        )
