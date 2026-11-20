"""Seed the small development catalogue without cluster jobs."""

from pathlib import Path

from django.conf import settings

from analyses.models import Analysis
from viruses.models import ViralCatalogue
from viruses.occurrences import ViralFragmentOccurrence
from workflows.flows.import_viral_catalogue_flow import import_catalogue


def seed_dev_viral_catalogue() -> ViralCatalogue:
    """Import the fixture and link its synthetic record to the development ASA.

    :return: The public development catalogue.
    """
    catalogue = import_catalogue.fn(
        results_directory=str(
            Path(settings.BASE_DIR)
            / "slurm-dev-environment/fs/nfs/production/catalogs/marine-viruses/v1.0"
        ),
        catalogue_id="marine-viruses-v1-0",
        name="Marine Viruses v1.0 (development fixture)",
        version="1.0",
        biome="Marine",
        ftp_url="http://localhost:8080/pub/databases/metagenomics/mgnify_viruses/marine-viruses/v1.0/",
        description="Development fixture with nine real marine fragments, three plasmids and one synthetic ASAv6 fragment (MGYV9999999999).",
        is_public=True,
    )
    analysis = Analysis.public_objects.filter(
        assembly__ena_accessions__contains=["ERZ857107"]
    ).first()
    if analysis:
        entry = catalogue.fragment_entries.select_related("fragment").get(
            fragment__accession="MGYV9999999999"
        )
        occurrence = ViralFragmentOccurrence.model_validate(
            entry.occurrences[0]
        ).model_copy(
            update={
                "analysis_id": analysis.pk,
                "analysis_accession": str(analysis.accession),
            }
        )
        entry.occurrences = [occurrence.model_dump()]
        entry.full_clean()
        entry.save(update_fields=["occurrences", "updated_at"])
    return catalogue
