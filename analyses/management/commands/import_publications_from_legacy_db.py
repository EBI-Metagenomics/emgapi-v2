import logging

from django.core.management.base import BaseCommand
from sqlalchemy import select

from analyses.models import Study, Publication
from workflows.data_io_utils.legacy_emg_dbs import (
    legacy_emg_db_session,
    LegacyPublication,
    LegacyStudyPublication,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Imports publications and their associations with studies from the legacy DB."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "-n",
            "--dry_run",
            action="store_true",
            help="If dry_run, importable data will be shown but not stored.",
        )

    def _make_publication(self, legacy_publication: LegacyPublication, dry_run: bool):
        if Publication.objects.filter(pubmed_id=legacy_publication.pubmed_id).exists():
            logger.info(
                f"Publication with PubMed ID {legacy_publication.pubmed_id} already exists, skipping."
            )
            return Publication.objects.get(pubmed_id=legacy_publication.pubmed_id)

        if dry_run:
            logger.info(
                f"Would make publication for ID {legacy_publication.pub_id} / {legacy_publication.pub_title}"
            )
            return None

        # Create metadata dictionary with all the additional fields
        metadata = Publication.PublicationMetadata(
            authors=legacy_publication.authors,
            doi=legacy_publication.doi,
            isbn=legacy_publication.isbn,
            iso_journal=legacy_publication.iso_journal,
            pub_type=legacy_publication.pub_type,
            volume=legacy_publication.volume,
            pubmed_central_id=legacy_publication.pubmed_central_id
            or legacy_publication.pubmed_id,
        )

        publication = Publication.objects.create(
            pubmed_id=legacy_publication.pubmed_id,
            title=legacy_publication.pub_title,
            published_year=legacy_publication.published_year,
            metadata=metadata,
        )
        logger.info(f"Created new publication object {publication}")
        return publication

    def _associate_studies(
        self,
        session,
        dry_run: bool,
    ):
        # Get all study-publication associations
        study_pubs_select_stmt = select(LegacyStudyPublication)

        count = 0
        for legacy_study_pub in session.execute(
            study_pubs_select_stmt
        ).scalars():  # type: LegacyStudyPublication
            try:
                study = Study.objects.get(id=legacy_study_pub.study_id)
                # We need to get the pubmed_id for the publication from the legacy DB
                legacy_pub = session.execute(
                    select(LegacyPublication).where(
                        LegacyPublication.pub_id == legacy_study_pub.pub_id
                    )
                ).scalar_one_or_none()

                if not legacy_pub:
                    logger.warning(
                        f"Legacy publication with ID {legacy_study_pub.pub_id} not found, skipping association"
                    )
                    continue

                if dry_run:
                    logger.info(
                        f"Would associate study {study.accession} with publication {legacy_pub.pub_title}"
                    )
                    count += 1
                    continue

                try:
                    publication = Publication.objects.get(
                        pubmed_id=legacy_pub.pubmed_id
                    )
                    publication.studies.add(study)
                    logger.info(
                        f"Associated study {study.accession} with publication {publication.pubmed_id}"
                    )
                    count += 1
                except Publication.DoesNotExist:
                    logger.warning(
                        f"Publication with PubMed ID {legacy_pub.pubmed_id} not found, skipping association"
                    )
            except Study.DoesNotExist:
                logger.warning(
                    f"Study with ID {legacy_study_pub.study_id} not found, skipping association"
                )
                continue

        return count

    def handle(self, *args, **options):
        dry_run = options.get("dry_run")
        with legacy_emg_db_session() as session:
            publications_select_stmt = select(LegacyPublication)

            publication_count = 0
            study_association_count = 0

            for legacy_publication in session.execute(
                publications_select_stmt
            ).scalars():  # type: LegacyPublication
                self._make_publication(legacy_publication, dry_run)
                publication_count += 1

            study_association_count = self._associate_studies(session, dry_run)

        logger.info(
            f"{'Would have' if dry_run else ''} Imported {publication_count} publications "
            f"with {study_association_count} study associations"
        )
