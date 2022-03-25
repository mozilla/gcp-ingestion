from concurrent.futures import ThreadPoolExecutor
from .helpers import IntegrationTest
from ingestion_edge.config import METADATA_HEADERS
from itertools import cycle, chain
from typing import Dict, Optional
import gzip
import pytest

# avoid having long strings in parametrized test
# names by outsourcing the actual values
DATA = {
    "GZIPPED": gzip.compress(b"data"),
    "MAX_LENGTH": b"x" * (1 * 1024 * 1024),  # nginx max data length 1MB
    "NON_UTF8": bytes(range(256)),
    "TOO_LONG": b"x" * (10 * 1024 * 1024 + 1),  # over pubsub max length of 10MB
    # identity values
    "": b"",
    "data": b"data",
}
HEADER = {
    "MAX_LENGTH": b"x" * 1024,
    "NON_UTF8": bytes(set(range(256)).difference(b"\r\n"))[::-1],
    "TOO_LONG": b"x" * 1025,
    # identity values
    None: None,
    "": b"",
    "header": b"header",
}
STRING = {
    "MAX_LENGTH": "x" * 1024,
    "NON_UTF8": bytes(set(range(256)).difference(b"\r\n"))[::-1].decode("latin"),
    "TOO_LONG": "x" * 1025,
    # identity values
    "": "",
    "x": "x",
    "args": "args",
    "p/a/t/h": "p/a/t/h",
}

# parameters for test_submit_success
args = ["", "args", "MAX_LENGTH"]
data = ["", "data", "GZIPPED", "NON_UTF8", "MAX_LENGTH"]
headers = [
    {key: value for key in METADATA_HEADERS if key != "content-length"}
    for value in [None, "", "header", "NON_UTF8"]
] + [{key: "MAX_LENGTH"} for key in METADATA_HEADERS if key != "content-length"]
uri_suffix = ["x", "p/a/t/h", "MAX_LENGTH"]


@pytest.mark.parametrize(
    "args,data,headers,uri_suffix",
    [
        case[:-1]  # drop the limiter value from the result
        for case in zip(
            # cycle over param lists to cover all values for each param
            cycle(args),
            cycle(data),
            cycle(headers),
            # args and uri_suffix will cycle in sync and that is okay
            cycle(uri_suffix),
            # limit zip to the length of the longest param list
            [None] * max(len(data), len(headers)),
        )
    ],
)
def test_submit_success(
    args: str,
    data: str,
    headers: Dict[str, Optional[str]],
    uri_suffix: str,
    integration_test: IntegrationTest,
):
    integration_test.args = STRING[args]
    integration_test.data = DATA[data]
    integration_test.headers = {key: HEADER[value] for key, value in headers.items()}
    uri_suffix = STRING[uri_suffix]
    if uri_suffix == STRING["MAX_LENGTH"]:
        min_uri_len = len(integration_test.uri_template.replace("<suffix:path>", ""))
        uri_suffix = uri_suffix[:-min_uri_len]
    integration_test.uri_suffix = uri_suffix
    integration_test.assert_accepted_and_delivered()


@pytest.mark.parametrize(
    "args,headers,uri_suffix",
    chain(
        [
            ("", {}, STRING["TOO_LONG"]),  # uri too long
            (STRING["TOO_LONG"], {}, "x"),  # args too long
        ],
        [
            ("", {key: HEADER["TOO_LONG"]}, "x")  # header too long
            for key in METADATA_HEADERS
            if key != "content-length"
        ],
    ),
)
def test_submit_invalid_header_too_long(
    args: str,
    headers: Dict[str, Optional[bytes]],
    uri_suffix: str,
    integration_test: IntegrationTest,
):
    if uri_suffix != "x" and "<suffix:path>" not in integration_test.uri_template:
        pytest.skip("requires suffix in uri_template")
    integration_test.args = args
    integration_test.headers = headers
    integration_test.uri_suffix = uri_suffix
    integration_test.assert_rejected(status=431)
    integration_test.assert_not_delivered()


def test_submit_success_concurrent_max_length_data(integration_test: IntegrationTest):
    integration_test.data = DATA["MAX_LENGTH"]
    with ThreadPoolExecutor(2) as executor:
        first = executor.submit(integration_test.assert_accepted)
        second = executor.submit(integration_test.assert_accepted)
        first.result()
        second.result()
    integration_test.assert_delivered(2)


def test_submit_invalid_payload_too_large(integration_test: IntegrationTest):
    integration_test.data = DATA["TOO_LONG"]
    integration_test.assert_rejected(status=413)
    integration_test.assert_not_delivered()
