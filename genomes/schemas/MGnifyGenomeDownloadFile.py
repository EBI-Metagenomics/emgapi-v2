from __future__ import annotations

import logging
from typing import Union

from ninja import Field
from typing_extensions import Annotated

from emgapiv2.api.downloads import MGnifyDownloadFile
from genomes import models as genome_models
from genomes.schemas.url_utils import build_public_url

logger = logging.getLogger(__name__)


class MGnifyGenomeDownloadFile(MGnifyDownloadFile):
    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[Union[int, str], Field(exclude=True)]

    url: str | None = None

    @staticmethod
    def resolve_url(obj: "MGnifyGenomeDownloadFile"):
        directory = MGnifyGenomeDownloadFile.resolve_result_directory(obj)
        return build_public_url(directory, obj.path) if directory else None

    @staticmethod
    def resolve_index_files(obj: "MGnifyGenomeDownloadFile"):
        directory = MGnifyGenomeDownloadFile.resolve_result_directory(obj)
        if not directory:
            return None
        return MGnifyDownloadFile.resolve_index_file_urls(
            obj, lambda path: build_public_url(directory, path)
        )

    @staticmethod
    def resolve_result_directory(obj: "MGnifyGenomeDownloadFile"):
        try:
            genome = genome_models.CatalogueGenome.objects.get(pk=obj.parent_identifier)
        except genome_models.CatalogueGenome.DoesNotExist:
            logger.warning(
                "No catalogue genome found with id %s for download URL resolution",
                obj.parent_identifier,
            )
            return None

        if not genome.result_directory:
            # Without a results directory, we cannot form a URL
            return None
        return genome.result_directory
