from collections.abc import Callable
from typing import Annotated
from urllib.parse import urljoin

from django.conf import settings
from ninja import Field, Schema

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileIndexFile,
)


class MGnifyDownloadFileIndexFile(Schema, DownloadFileIndexFile):
    path: Annotated[str, Field(exclude=True)]
    url: str | None = Field(
        None,
        description="Full URL of the index file.",
        examples=["https://www.ebi.ac.uk/metagenomics/path/to/annotations.tsv.gz.gzi"],
    )


class MGnifyDownloadFile(Schema, DownloadFile):
    """Expose stored single or multiple indexes as a list of URL-bearing indexes."""

    index_file: Annotated[
        DownloadFileIndexFile | list[DownloadFileIndexFile] | None,
        Field(exclude=True),
    ] = None
    index_files: list[MGnifyDownloadFileIndexFile] | None = Field(
        None,
        examples=[
            [
                {
                    "index_type": "gzi",
                    "url": urljoin(
                        settings.EMG_CONFIG.service_urls.transfer_services_url_root,
                        "annotations.tsv.gz.gzi",
                    ),
                }
            ]
        ],
    )

    @staticmethod
    def resolve_index_file_urls(
        obj: DownloadFile, build_url: Callable[[str], str | None]
    ) -> list[MGnifyDownloadFileIndexFile] | None:
        if obj.index_file is None:
            return None
        indexes = (
            [obj.index_file]
            if isinstance(obj.index_file, DownloadFileIndexFile)
            else obj.index_file
        )
        return [
            MGnifyDownloadFileIndexFile.model_validate(
                {**index.model_dump(), "url": build_url(index.path)}
            )
            for index in indexes
        ]
