import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
from pytest_httpx import HTTPXMock
import httpx

from oai_pmh_client import (
    OAIClient,
    BadArgumentError,
    Identify,
    Header,
    MetadataFormat,
    Set,
    Record,
)

# Using arXiv as the test endpoint for integration tests.
BASE_URL = "https://export.arxiv.org/oai2"
CANONICAL_BASE_URL = "https://oaipmh.arxiv.org/oai"


@pytest.fixture
def client():
    """
    Returns an OAIClient instance for testing.
    """
    return OAIClient(BASE_URL)


@pytest.fixture
def mock_client_get(httpx_mock: HTTPXMock):
    """
    Returns an OAIClient instance with a mocked HTTPX client using GET.
    """
    return OAIClient(BASE_URL, use_post=False)


@pytest.fixture
def mock_client_post(httpx_mock: HTTPXMock):
    """
    Returns an OAIClient instance with a mocked HTTPX client using POST.
    """
    return OAIClient(BASE_URL, use_post=True)


def load_test_data(filename: str) -> bytes:
    """
    Loads test data from the tests/data directory.
    """
    return (Path(__file__).parent / "data" / filename).read_bytes()


# The following tests are integration tests and will make live HTTP requests.
# They are marked with 'integration' and can be skipped with `pytest -m "not integration"`.


@pytest.mark.integration
def test_identify(client: OAIClient):
    """
    Tests the identify method against a live endpoint.
    """
    response = client.identify()
    assert isinstance(response, Identify)
    assert response.repository_name == "arXiv"
    assert response.base_url == CANONICAL_BASE_URL
    assert response.protocol_version == "2.0"


@pytest.mark.integration
def test_list_metadata_formats(client: OAIClient):
    """
    Tests the list_metadata_formats method against a live endpoint.
    """
    formats = list(client.list_metadata_formats())
    assert len(formats) > 0
    assert all(isinstance(f, MetadataFormat) for f in formats)
    prefixes = [f.prefix for f in formats]
    assert "oai_dc" in prefixes


@pytest.mark.integration
def test_list_sets(client: OAIClient):
    """
    Tests the list_sets method against a live endpoint.
    """
    sets = list(client.list_sets())
    assert len(sets) > 0
    assert all(isinstance(s, Set) for s in sets)


@pytest.mark.integration
def test_get_record(client: OAIClient):
    """
    Tests the get_record method against a live endpoint.
    """
    identifier = "oai:arXiv.org:cs/0012001"
    record = client.get_record(identifier, "oai_dc")
    assert isinstance(record, Record)
    assert record.header is not None
    assert record.header.identifier == identifier
    assert not record.header.is_deleted
    assert record.metadata is not None


@pytest.mark.integration
def test_list_identifiers(client: OAIClient):
    """
    Tests the list_identifiers method against a live endpoint.
    """
    # Take just a few items to avoid fetching the whole list
    from itertools import islice

    identifiers = list(
        islice(client.list_identifiers(metadata_prefix="oai_dc", set_spec="cs"), 5)
    )
    assert len(identifiers) > 0
    assert all(isinstance(i, Header) for i in identifiers)


@pytest.mark.integration
def test_list_records(client: OAIClient):
    """
    Tests the list_records method against a live endpoint.
    """
    # Take just a few items to avoid fetching the whole list
    from itertools import islice

    records = list(
        islice(client.list_records(metadata_prefix="oai_dc", set_spec="cs"), 5)
    )
    assert len(records) > 0
    assert all(isinstance(r, Record) for r in records)


# The following tests are unit tests using mocked responses.


def test_oai_error(mock_client_get: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client raises the correct exception for an OAI error.
    """
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=invalid",
        content=load_test_data("error_bad_argument.xml"),
    )
    with pytest.raises(BadArgumentError):
        list(mock_client_get.list_records(metadata_prefix="invalid"))


def test_list_records_auto_granularity_with_time(
    mock_client_get: OAIClient, httpx_mock: HTTPXMock
):
    """Default auto granularity preserves second-level precision when time is present."""
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc&from=2024-01-01T12%3A00%3A00Z",
        content=load_test_data("list_records_final.xml"),
    )
    from_date = datetime(2024, 1, 1, 12, 0, 0)
    records = list(
        mock_client_get.list_records(metadata_prefix="oai_dc", from_date=from_date)
    )
    assert len(records) == 1
    assert isinstance(records[0], Record)


def test_list_records_auto_granularity_midnight(
    mock_client_get: OAIClient, httpx_mock: HTTPXMock
):
    """Auto granularity falls back to day-level formatting for midnight datetimes."""
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc&from=2024-01-01",
        content=load_test_data("list_records_final.xml"),
    )
    from_date = datetime(2024, 1, 1, 0, 0, 0)
    records = list(
        mock_client_get.list_records(metadata_prefix="oai_dc", from_date=from_date)
    )
    assert len(records) == 1
    assert isinstance(records[0], Record)


def test_list_records_force_day_granularity(httpx_mock: HTTPXMock):
    """Explicitly forcing day-level granularity maintains prior behaviour."""
    client = OAIClient(BASE_URL, datestamp_granularity="YYYY-MM-DD")
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc&from=2024-01-01",
        content=load_test_data("list_records_final.xml"),
    )
    from_date = datetime(2024, 1, 1, 12, 0, 0)
    records = list(client.list_records(metadata_prefix="oai_dc", from_date=from_date))
    assert len(records) == 1
    assert isinstance(records[0], Record)


def test_list_records_with_datetime_seconds_granularity(httpx_mock: HTTPXMock):
    """Tests second-level granularity when explicitly requested."""
    client = OAIClient(BASE_URL, datestamp_granularity="YYYY-MM-DDThh:mm:ssZ")
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc&from=2024-01-01T12%3A00%3A00Z",
        content=load_test_data("list_records_final.xml"),
    )
    from_date = datetime(2024, 1, 1, 12, 0, 0)
    records = list(client.list_records(metadata_prefix="oai_dc", from_date=from_date))
    assert len(records) == 1
    assert isinstance(records[0], Record)


def test_list_records_with_resumption(
    mock_client_get: OAIClient, httpx_mock: HTTPXMock
):
    """
    Tests that the client correctly handles resumption tokens with GET.
    """
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc",
        content=load_test_data("list_records_resumption.xml"),
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&resumptionToken=token123",
        content=load_test_data("list_records_final.xml"),
    )
    records = list(mock_client_get.list_records(metadata_prefix="oai_dc"))
    assert len(records) == 2
    assert records[0].header is not None
    assert records[0].header.identifier == "oai:example.org:1"
    assert records[1].header is not None
    assert records[1].header.identifier == "oai:example.org:2"


def test_list_records_with_resumption_post(
    mock_client_post: OAIClient, httpx_mock: HTTPXMock
):
    """
    Tests that the client correctly handles resumption tokens with POST.
    """
    httpx_mock.add_response(
        method="POST",
        url=BASE_URL,
        content=load_test_data("list_records_resumption.xml"),
        match_content=b"verb=ListRecords&metadataPrefix=oai_dc",
    )
    httpx_mock.add_response(
        method="POST",
        url=BASE_URL,
        content=load_test_data("list_records_final.xml"),
        match_content=b"verb=ListRecords&resumptionToken=token123",
    )
    records = list(mock_client_post.list_records(metadata_prefix="oai_dc"))
    assert len(records) == 2
    assert records[0].header is not None
    assert records[0].header.identifier == "oai:example.org:1"
    assert records[1].header is not None
    assert records[1].header.identifier == "oai:example.org:2"


def test_list_records_with_deletion(mock_client_get: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client correctly handles deleted records.
    """
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc",
        content=load_test_data("list_records_with_deletion.xml"),
    )
    records = list(mock_client_get.list_records(metadata_prefix="oai_dc"))
    assert len(records) == 2

    # Check the deleted record
    deleted_record = records[0]
    assert deleted_record.header is not None
    assert deleted_record.header.identifier == "oai:example.org:deleted-record"
    assert deleted_record.header.is_deleted
    assert deleted_record.metadata is None

    # Check the active record
    active_record = records[1]
    assert active_record.header is not None
    assert active_record.header.identifier == "oai:example.org:active-record"
    assert not active_record.header.is_deleted
    assert active_record.metadata is not None


def test_record_without_header(mock_client_get: OAIClient, httpx_mock: HTTPXMock):
    """
    Tests that the client correctly handles records without headers.
    """
    from lxml import etree
    from oai_pmh_client.models import Record

    # Load and parse the test XML
    xml_content = load_test_data("record_no_header.xml")
    root = etree.fromstring(xml_content)

    # Find the record element
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    record_element = root.find(".//oai:record", namespaces=ns)

    # Parse the record
    record = Record.from_xml(record_element)

    assert isinstance(record, Record)
    assert record.header is None
    assert record.metadata is not None


def test_debug_logging_emits_request_urls(httpx_mock: HTTPXMock, caplog):
    client = OAIClient(BASE_URL, use_post=False)
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc",
        content=load_test_data("list_records_final.xml"),
    )
    with caplog.at_level(logging.DEBUG):
        list(client.list_records(metadata_prefix="oai_dc"))

    assert any(
        "OAI request: GET" in message and f"{BASE_URL}?verb=ListRecords" in message
        for message in caplog.messages
    )


def test_request_retries_on_timeout(httpx_mock: HTTPXMock):
    client = OAIClient(
        BASE_URL,
        use_post=False,
        max_request_retries=2,
        request_backoff_factor=0.0,
    )
    httpx_mock.add_exception(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc",
        exception=httpx.ReadTimeout("Read timed out"),
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc",
        content=load_test_data("list_records_final.xml"),
    )

    records = list(client.list_records(metadata_prefix="oai_dc"))
    assert len(records) == 1


def test_mixed_granularity_bug(httpx_mock: HTTPXMock):
    """
    Reproduces the bug where mixed granularity (one date at midnight, one with time)
    results in inconsistent granularity in the request URL.
    """
    client = OAIClient(BASE_URL)

    # from_date is at midnight (00:00:00)
    from_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    # until_date has a time component
    until_date = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    # We expect the client to use seconds granularity for BOTH because one of them has time.

    # We mock the response to avoid actual network call, but we want to inspect the request URL.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc&from=2024-01-01T00%3A00%3A00Z&until=2024-01-02T12%3A00%3A00Z",
        content=b"<root></root>",  # Dummy content
    )

    try:
        list(
            client.list_records(
                metadata_prefix="oai_dc", from_date=from_date, until_date=until_date
            )
        )
    except Exception:
        pass

    # Let's inspect the actual request made
    request = httpx_mock.get_request()
    assert request is not None

    url = request.url

    # We want to assert that we get the CORRECT behavior (consistent granularity).
    # So we assert that 'from' parameter contains 'T' (indicating time component)
    assert "from=2024-01-01T00%3A00%3A00Z" in str(
        url
    ) or "from=2024-01-01T00:00:00Z" in str(url)


def test_timezone_aware_midnight_granularity(httpx_mock: HTTPXMock):
    """
    Tests that a timezone-aware datetime that is midnight in local time
    but NOT midnight in UTC results in seconds-level granularity.
    """
    client = OAIClient(BASE_URL)

    # Midnight CET (UTC+1) is 23:00 UTC previous day.
    # Should be treated as having time component.
    cet = timezone(timedelta(hours=1))
    from_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=cet)

    # We expect seconds granularity: 2023-12-31T23:00:00Z
    expected_param = "from=2023-12-31T23%3A00%3A00Z"

    httpx_mock.add_response(
        method="GET",
        url=f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc&{expected_param}",
        content=b"<root></root>",
    )

    try:
        list(client.list_records(metadata_prefix="oai_dc", from_date=from_date))
    except Exception:
        pass

    request = httpx_mock.get_request()
    assert request is not None

    assert expected_param in str(request.url)
