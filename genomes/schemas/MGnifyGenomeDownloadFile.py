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


class MGnifyGenomeDownloadFile(Schema, DownloadFile):
    path: Annotated[str, Field(exclude=True)]
    parent_identifier: Annotated[Union[int, str], Field(exclude=True)]

    url: str | None = None

    @staticmethod
    def resolve_url(obj: "MGnifyGenomeDownloadFile"):
        try:
            genome = genome_models.Genome.objects.get(accession=obj.parent_identifier)
        except genome_models.Genome.DoesNotExist:
            logger.warning(
                f"No Genome found with accession {obj.parent_identifier} for download URL resolution"
            )
            return None

        if not genome.result_directory:
            # Without a results directory, we cannot form a URL
            return None

        return urljoin(
            EMG_CONFIG.service_urls.transfer_services_url_root,
            urljoin(trailing_slash_ensured_dir(genome.result_directory), obj.path),
        )
