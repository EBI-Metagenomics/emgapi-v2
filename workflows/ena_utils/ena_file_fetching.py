import logging

from django.conf import settings

logger = logging.getLogger(__name__)

EMG_CONFIG = settings.EMG_CONFIG


def convert_ena_ftp_to_fire_fastq(
    ftp_url: str, raise_if_not_convertible: bool = False
) -> str:
    """
    Converts an ENA FTP URL to a FIRE s3:// URL.

    If the URL does not start with the expected FTP prefix,
    it can either raise an error or log a warning based on the
    `raise_if_not_convertible` flag.

    :param ftp_url: The ENA FTP URL to be converted.
    :param raise_if_not_convertible: Flag to determine whether to raise an
        exception if the URL is not convertible. Default is False.
    :return: The converted FIRE-FASTQ compatible URL, or the original URL if
        it was not convertible.
    """
    fire_url = ftp_url
    protocol_and_parts = fire_url.split("://")
    fire_url = protocol_and_parts[-1]  # without e.g. http:// if present

    if not fire_url.startswith(EMG_CONFIG.ena.ftp_prefix):
        if raise_if_not_convertible:
            raise Exception(f"{ftp_url} is not convertible to FIRE.")
        else:
            logger.warning(
                f"Expected {ftp_url} to start with '{EMG_CONFIG.ena.ftp_prefix}'. Not converting to FIRE."
            )
            return ftp_url

    return fire_url.replace(EMG_CONFIG.ena.ftp_prefix, EMG_CONFIG.ena.fire_prefix)
