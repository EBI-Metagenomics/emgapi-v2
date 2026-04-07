from .settings import *  # noqa: F401, F403

# Use custom DB backend that triggers connection health checks outside HTTP
# request cycles. The standard Django middleware handles this for the web API;
# here we need it for long-running Prefect flows that pause for hours/days
# waiting on cluster jobs.
DATABASES["default"]["ENGINE"] = "emgapiv2.db_backend.postgresql"  # noqa: F405
