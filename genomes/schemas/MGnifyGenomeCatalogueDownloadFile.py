from __future__ import annotations

import logging
from typing import Union
from urllib.parse import urljoin

from django.conf import settings
from ninja import Field, Schema
from typing_extensions import Annotated

from analyses.base_models.with_downloads_models import DownloadFile
from genomes import models as genome_models
from workflows.data_io_utils.filenames import trailing_slash_ensured_dir

logger = logging.getLogger(__name__)
EMG_CONFIG = settings.EMG_CONFIG


class MGnifyGenomeCatalogueDownloadFile(Schema, DownloadFile):
    """
    Download file representation for GenomeCatalogue objects.

    Resolves a public URL using the catalogue's result_directory, if available.
    """

    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[Union[int, str], Field(exclude=True)]

    url: str | None = None

    @staticmethod
    def resolve_url(obj: "MGnifyGenomeCatalogueDownloadFile"):
        try:
            catalogue = genome_models.GenomeCatalogue.objects.get(
                catalogue_id=obj.parent_identifier
            )
        except genome_models.GenomeCatalogue.DoesNotExist:
            logger.warning(
                "No GenomeCatalogue found with catalogue_id %s for download URL resolution",
                obj.parent_identifier,
            )
            return None

        if not catalogue.result_directory:
            # Without a results directory, we cannot form a URL
            return None

        return urljoin(
            EMG_CONFIG.service_urls.transfer_services_url_root,
            urljoin(trailing_slash_ensured_dir(catalogue.result_directory), obj.path),
        )
