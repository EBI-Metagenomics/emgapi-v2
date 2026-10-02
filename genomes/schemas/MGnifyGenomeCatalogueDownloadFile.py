from __future__ import annotations

import logging
from typing import Union

from ninja import Field
from typing_extensions import Annotated

from emgapiv2.api.downloads import MGnifyDownloadFile
from genomes import models as genome_models
from genomes.schemas.url_utils import build_public_url

logger = logging.getLogger(__name__)


class MGnifyGenomeCatalogueDownloadFile(MGnifyDownloadFile):
    """
    Download file representation for GenomeCatalogue objects.

    Resolves a public URL using the catalogue's result_directory, if available.
    """

    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[Union[int, str], Field(exclude=True)]

    url: str | None = None

    @staticmethod
    def resolve_url(obj: "MGnifyGenomeCatalogueDownloadFile"):
        directory = MGnifyGenomeCatalogueDownloadFile.resolve_result_directory(obj)
        return build_public_url(directory, obj.path) if directory else None

    @staticmethod
    def resolve_index_files(obj: "MGnifyGenomeCatalogueDownloadFile"):
        directory = MGnifyGenomeCatalogueDownloadFile.resolve_result_directory(obj)
        if not directory:
            return None
        return MGnifyDownloadFile.resolve_index_file_urls(
            obj, lambda path: build_public_url(directory, path)
        )

    @staticmethod
    def resolve_result_directory(obj: "MGnifyGenomeCatalogueDownloadFile"):
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
            logger.warning(
                "Catalogue result directory not found for catalogue id %s for download URL resolution",
                obj.parent_identifier,
            )
            return None

        return catalogue.result_directory
