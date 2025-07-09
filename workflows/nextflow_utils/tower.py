from django.conf import settings
from pydantic_core import Url


def get_nextflow_tower_url() -> Url | None:
    return Url(
        f"https://cloud.seqera.io/orgs/{settings.EMG_CONFIG.slurm.nextflow_tower_org}/workspaces/{settings.EMG_CONFIG.slurm.nextflow_tower_workspace}/watch"
    )
