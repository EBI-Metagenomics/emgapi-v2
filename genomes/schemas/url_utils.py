from __future__ import annotations

from pathlib import Path
from urllib.parse import urljoin

from django.conf import settings

from workflows.data_io_utils.filenames import trailing_slash_ensured_dir


def build_transfer_service_public_url(
    result_directory: str | Path | None, file_path: str | Path | None
) -> str | None:
    """
    Build a public download URL for a file that lives under a results directory
    exposed by the Transfer Service.
    - Ensures the joined path is relative (no leading slash) so the base path is preserved.
    """
    transfer_service_root_url = (
        settings.EMG_CONFIG.service_urls.transfer_services_url_root
    )
    # Build a relative path under the result directory ensuring no leading slash
    relative_results_path = (
        f"{trailing_slash_ensured_dir(result_directory).lstrip('/')}"
        f"{str(file_path).lstrip('/')}"
    )

    return urljoin(transfer_service_root_url, relative_results_path)
