from django.conf import settings
from nginx_secure_links.storages import FileStorage

private_storage = FileStorage(
    # File storage for private data - these are served by ngninx secure links
    location=settings.EMG_CONFIG.slurm.shared_filesystem_root_on_server,
    base_url=settings.EMG_CONFIG.service_urls.private_data_url_root,
)
