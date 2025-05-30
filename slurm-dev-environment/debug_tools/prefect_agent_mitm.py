import os
from mitmproxy import http

"""
This file configures the MITM rules present in the dev prefect-agent.
It can therefore be used to simulate network interruptions.
It uses flag files for conditional routing, so that e.g.
a Taskfile task can touch a file in the container to temporarily
alter the routing rules.
"""

INTERCEPT_404_PATH = "/tmp/prefect_is_404"


def request(flow: http.HTTPFlow):
    api = os.getenv("PREFECT_API_URL")
    if not api:
        raise UserWarning("PREFECT_API_URL env var not set")

    # If the flag file exists, return 404 for API requests
    if os.path.exists(INTERCEPT_404_PATH) and flow.request.pretty_url.startswith(api):
        flow.response = http.Response.make(
            404,  # HTTP status code
            b"Not Found",  # Response body
            {"Content-Type": "text/plain"},  # Headers
        )
        return
