import tempfile

from .settings import *

EMG_CONFIG.slurm.default_workdir = tempfile.gettempdir()
EMG_CONFIG.webin.emg_webin_account = "webin-fake"
EMG_CONFIG.webin.emg_webin_password = "not-a-pw"
EMG_CONFIG.webin.dcc_account = "dcc_fake"
EMG_CONFIG.webin.dcc_password = "not-a-dcc-pw"
EMG_CONFIG.ena.portal_search_api_max_retries = 0  # failfast in unit tests
EMG_CONFIG.ena.portal_search_api_retry_delay_seconds = 1
EMG_CONFIG.amplicon_pipeline.allow_non_insdc_run_names = True
EMG_CONFIG.amplicon_pipeline.keep_study_summary_partials = True

STORAGES = {
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedStaticFilesStorage",
    },
}
