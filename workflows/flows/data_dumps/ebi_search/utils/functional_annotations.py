import gzip
import logging
import re
import time
from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import quote

import httpx

from analyses.base_models.with_downloads_models import DownloadFile, DownloadType
from analyses.models import Analysis


@dataclass(frozen=True)
class RetryPolicy:
    total: int
    backoff_factor: float
    status_forcelist: set[int]

    def sleep(self, attempt_number: int) -> None:
        if attempt_number <= 0 or self.backoff_factor <= 0:
            return
        time.sleep(self.backoff_factor * attempt_number)


DOWNLOAD_RETRY = RetryPolicy(
    total=4,
    backoff_factor=1,
    status_forcelist={429, 500, 502, 503, 504},
)

IDENTIFIER_PATTERNS = {
    "go": re.compile(r"\bGO:\d{7}\b"),
    "interpro": re.compile(r"\bIPR\d{6}\b"),
    "rhea": re.compile(r"\bRHEA:\d+\b"),
}

FUNCTIONAL_DOWNLOAD_GROUP_KINDS = {
    f"{Analysis.FUNCTIONAL_ANNOTATION}.go_slims": "go",
    f"{Analysis.FUNCTIONAL_ANNOTATION}.rhea_reactions": "rhea",
}

# V4/V5 functional downloads use the generic "all" group, and current InterPro
# raw and summary files share a group, so their semantic descriptions disambiguate them.
FUNCTIONAL_DOWNLOAD_DESCRIPTION_KINDS = {
    "InterPro Identifier counts": "interpro",
    "Legacy InterPro summary": "interpro",
    "InterPro summary": "interpro",
    "GO Term counts": "go",
    "GO-Slim Term counts": "go",
    "Legacy GO summary": "go",
    "Complete GO annotation": "go",
    "GO slim annotation": "go",
    "Rhea reaction counts": "rhea",
}


# Select indexable GO, InterPro, and Rhea summary files using metadata, not filenames.
def _functional_download_kind(download: DownloadFile) -> str | None:
    if download.download_type != DownloadType.FUNCTIONAL_ANALYSIS:
        return None
    return FUNCTIONAL_DOWNLOAD_GROUP_KINDS.get(
        download.download_group
    ) or FUNCTIONAL_DOWNLOAD_DESCRIPTION_KINDS.get(download.short_description)


def _http_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=True,
        timeout=httpx.Timeout(120, connect=10),
    )


def _retry_request(
    client: httpx.Client, url: str, retry: RetryPolicy
) -> httpx.Response:
    last_response = None
    for attempt_number in range(retry.total + 1):
        response = client.get(url)
        if response.status_code not in retry.status_forcelist:
            return response
        last_response = response
        if attempt_number == retry.total:
            return response
        retry.sleep(attempt_number + 1)
    return last_response


def _identifiers_from_url(
    client: httpx.Client,
    url: str,
    path: str,
    kind: str,
    analysis_accession: str,
    run_logger: logging.Logger,
) -> set[str]:
    try:
        response = _retry_request(client, url, DOWNLOAD_RETRY)
        response.raise_for_status()
        contents = response.content
        if path.endswith(".gz"):
            contents = gzip.decompress(contents)
        return set(IDENTIFIER_PATTERNS[kind].findall(contents.decode("utf-8")))
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            run_logger.warning(
                "Skipping missing %s file for %s: %s",
                kind,
                analysis_accession,
                url,
            )
            return set()
        raise


def functional_cross_references(
    analysis: Analysis,
    transfer_services_url_root: str,
    client: httpx.Client,
    run_logger: logging.Logger,
) -> dict[str, set[str]]:
    identifiers = {kind: set() for kind in IDENTIFIER_PATTERNS}
    candidates = []
    for download in analysis.downloads_as_objects:
        if kind := _functional_download_kind(download):
            candidates.append((kind, str(download.path)))

    if candidates and not analysis.external_results_dir:
        run_logger.warning(
            "Skipping functional files for %s because external_results_dir is empty",
            analysis.accession,
        )
        return identifiers

    for kind, path in candidates:
        relative_path = str(PurePosixPath(analysis.external_results_dir) / path)
        url = f"{transfer_services_url_root}{quote(relative_path, safe='/')}"
        identifiers[kind].update(
            _identifiers_from_url(
                client, url, path, kind, analysis.accession, run_logger
            )
        )
    return identifiers
