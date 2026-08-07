from __future__ import annotations

import logging
from typing import Union

from ninja import Field, Schema
from typing_extensions import Annotated

from analyses.base_models.with_downloads_models import DownloadFile
from genomes import models as genome_models
from genomes.schemas.url_utils import build_public_url

logger = logging.getLogger(__name__)


class MGnifyGenomeDownloadFile(Schema, DownloadFile):
    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[Union[int, str], Field(exclude=True)]

    url: str | None = None

    @staticmethod
    def resolve_url(obj: "MGnifyGenomeDownloadFile"):
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
        return build_public_url(genome.result_directory, obj.path)
