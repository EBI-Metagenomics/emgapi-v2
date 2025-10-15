import re

import pytest


@pytest.fixture
def ena_any_sample_metadata(httpx_mock):
    httpx_mock.add_response(
        url=re.compile(
            r".*result=sample.*specimen_voucher.*"  # arbitrary sample query - only used when syncing sample metadata from ENA
        ),
        json=[{"sample_title": "any sample", "lat": 1}],
        is_reusable=True,
        is_optional=True,
    )
