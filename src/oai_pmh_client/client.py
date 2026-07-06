import logging
import time
from datetime import datetime, timezone
from typing import Union, Iterator, Literal, Sequence

import httpx
from lxml import etree

from .exceptions import (
    OAIError,
    BadArgumentError,
    BadResumptionTokenError,
    BadVerbError,
    CannotDisseminateFormatError,
    IdDoesNotExistError,
    NoRecordsMatchError,
    NoMetadataFormatsError,
    NoSetHierarchyError,
    ResumptionTokenFailedError,
)
from .models import (
    Identify,
    Header,
    MetadataFormat,
    Set,
    Record,
    ResumptionToken,
    NS,
)

logger = logging.getLogger(__name__)

OAI_ERROR_MAP = {
    "badArgument": BadArgumentError,
    "badResumptionToken": BadResumptionTokenError,
    "badVerb": BadVerbError,
    "cannotDisseminateFormat": CannotDisseminateFormatError,
    "idDoesNotExist": IdDoesNotExistError,
    "noRecordsMatch": NoRecordsMatchError,
    "noMetadataFormats": NoMetadataFormatsError,
    "noSetHierarchy": NoSetHierarchyError,
}

# A transport error can occur before a request reaches the server, in which
# case the resumption token it carries is still valid. It can also occur after
# the server has processed the request (e.g. the connection dropped
# mid-response), in which case the token is already consumed on servers that
# issue single-use tokens. Retrying once covers the first case without
# repeatedly replaying a token that is most likely burned.
TOKEN_REQUEST_TRANSIENT_RETRIES = 1

Datestamp = Union[datetime, str]
DatestampGranularity = Literal["auto", "YYYY-MM-DD", "YYYY-MM-DDThh:mm:ssZ"]
VALID_GRANULARITIES: set[str] = {"auto", "YYYY-MM-DD", "YYYY-MM-DDThh:mm:ssZ"}


class OAIClient:
    """
    A client for interacting with an OAI-PMH repository.
    """

    def __init__(
        self,
        base_url: str,
        client: httpx.Client | None = None,
        timeout: int | httpx.Timeout = 20,
        use_post: bool = False,
        datestamp_granularity: DatestampGranularity = "auto",
        max_request_retries: int = 3,
        request_backoff_factor: float = 0.5,
        request_max_backoff: float = 5.0,
        max_transient_retries: int = 3,
    ):
        """
        Initializes the OAIClient.

        :param base_url: The base URL of the OAI-PMH repository.
        :param client: An optional httpx.Client instance.
        :param timeout: The timeout for HTTP requests in seconds.
        :param use_post: Whether to use POST requests instead of GET.
        :param datestamp_granularity: The granularity to use when formatting datetime
            objects for selective harvesting. Valid values per the OAI-PMH spec are
            "YYYY-MM-DD" (day-level) and "YYYY-MM-DDThh:mm:ssZ" (second-level, UTC).
            Use "auto" (default) to automatically select day-level for midnight
            datetimes and second-level when any time component is present.
        :param max_request_retries: The maximum number of attempts for a request
            that fails with an httpx timeout exception.
        :param request_backoff_factor: The base delay in seconds for exponential
            backoff between retries.
        :param request_max_backoff: The maximum delay in seconds between retries.
        :param max_transient_retries: The number of times to retry a request
            that fails with an empty or unparseable response body, a transport
            error, or a 5xx response. Applies in full to initial requests only:
            requests carrying a resumption token are retried at most once for
            transport errors and 5xx responses, and never for empty bodies,
            because many servers issue single-use tokens that are consumed by
            the failed request (see ResumptionTokenFailedError). Retries use
            exponential backoff governed by request_backoff_factor and
            request_max_backoff. Timeouts are retried separately, governed
            by max_request_retries only. Set to 0 to disable these retries.
        """
        self.base_url = base_url
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=True)
        self.use_post = use_post
        self.max_request_retries = max(1, max_request_retries)
        self.request_backoff_factor = max(0.0, request_backoff_factor)
        self.request_max_backoff = max(0.0, request_max_backoff)
        self.max_transient_retries = max(0, max_transient_retries)
        if datestamp_granularity not in VALID_GRANULARITIES:
            raise ValueError(
                "datestamp_granularity must be one of 'auto', 'YYYY-MM-DD', or 'YYYY-MM-DDThh:mm:ssZ'"
            )
        self.datestamp_granularity = datestamp_granularity

    def _determine_granularity(self, dt: datetime) -> str:
        if self.datestamp_granularity == "auto":
            has_time_component = any((dt.hour, dt.minute, dt.second, dt.microsecond))
            return "YYYY-MM-DDThh:mm:ssZ" if has_time_component else "YYYY-MM-DD"
        return self.datestamp_granularity

    def _determine_granularity_for_dates(
        self, dates: Sequence[Datestamp | None]
    ) -> str:
        if self.datestamp_granularity != "auto":
            return self.datestamp_granularity

        for dt in dates:
            if isinstance(dt, datetime):
                # Ensure we check granularity against UTC time
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt = dt.astimezone(timezone.utc)

                if any((dt.hour, dt.minute, dt.second, dt.microsecond)):
                    return "YYYY-MM-DDThh:mm:ssZ"
        return "YYYY-MM-DD"

    def _format_datestamp(self, dt: Datestamp, granularity: str | None = None) -> str:
        """
        Formats a datetime object into an OAI-PMH datestamp string.
        """
        if isinstance(dt, str):
            return dt
        if dt.tzinfo is None:
            # If the datetime object is naive, assume it's in UTC.
            dt = dt.replace(tzinfo=timezone.utc)
        # If the datetime object is aware, convert it to UTC.
        dt = dt.astimezone(timezone.utc)

        if granularity is None:
            granularity = self._determine_granularity(dt)

        if granularity == "YYYY-MM-DD":
            return dt.strftime("%Y-%m-%d")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _request(self, verb: str, **kwargs) -> etree._Element:
        """
        Makes a request to the OAI-PMH repository and returns the parsed XML.

        :param verb: The OAI-PMH verb.
        :param kwargs: Additional request parameters.
        :return: The parsed XML response.
        """
        params: dict[str, str | None] = {"verb": verb}
        # Filter out None values so they aren't included in the query
        for key, value in kwargs.items():
            if value is not None:
                params[key] = value

        if self.use_post:
            request = self._client.build_request("POST", self.base_url, data=params)
        else:
            request = self._client.build_request("GET", self.base_url, params=params)

        logger.debug("OAI request: %s %s", request.method, request.url)

        is_token_request = "resumptionToken" in params
        xml = self._send_and_parse_with_retries(request, verb, is_token_request)

        error = xml.find("oai:error", namespaces=NS)
        if error is not None:
            code = error.get("code", "")
            message = error.text or ""
            exception_class = OAI_ERROR_MAP.get(code, OAIError)
            raise exception_class(message)

        return xml

    def _send_and_parse_with_retries(
        self, request: httpx.Request, verb: str, is_token_request: bool
    ) -> etree._Element:
        """
        Sends a request and parses the response, retrying transient failures.

        Transient failures are empty or unparseable response bodies (some
        servers, e.g. Axiell Adlib, intermittently return HTTP 200 with an
        empty body under load), transport errors, and 5xx responses. Initial
        requests are retried up to ``max_transient_retries`` times with
        exponential backoff.

        Requests carrying a resumption token are treated differently, because
        many servers issue single-use, session-bound tokens that are consumed
        as soon as the server processes a request. An empty body proves the
        server processed the request, so the token request is not retried at
        all; a transport error or 5xx response is retried once, since the
        request may never have reached the server. When a token request
        fails, ResumptionTokenFailedError is raised so callers can restart
        the list operation from the beginning.

        Timeouts are retried by _send_with_retries (up to
        ``max_request_retries``) and are not retried again here. 4xx
        responses and OAI protocol errors are never retried.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._send_with_retries(request)
                response.raise_for_status()
                return etree.fromstring(response.content)
            except etree.XMLSyntaxError as error:
                if is_token_request:
                    raise ResumptionTokenFailedError(
                        "Received an empty or unparseable response to a "
                        "resumption token request; the token may have been "
                        "consumed. Restart the list operation to recover."
                    ) from error
                if attempt > self.max_transient_retries:
                    raise
                self._backoff_before_retry(verb, error, attempt, is_token_request)
            except httpx.HTTPStatusError as error:
                if error.response.status_code < 500:
                    raise
                if attempt > self._transient_retry_limit(is_token_request):
                    if is_token_request:
                        raise ResumptionTokenFailedError(
                            "A resumption token request failed with a 5xx "
                            "response; the token may have been consumed. "
                            "Restart the list operation to recover."
                        ) from error
                    raise
                self._backoff_before_retry(verb, error, attempt, is_token_request)
            except httpx.TimeoutException as error:
                # Timeouts are already retried by _send_with_retries, up to
                # max_request_retries. Retrying them here as well would
                # multiply the number of attempts beyond what callers
                # configured, so a timeout that has exhausted those retries
                # is not retried again.
                if is_token_request:
                    raise ResumptionTokenFailedError(
                        "A resumption token request failed with a timeout; "
                        "the token may have been consumed. Restart the list "
                        "operation to recover."
                    ) from error
                raise
            except httpx.TransportError as error:
                if attempt > self._transient_retry_limit(is_token_request):
                    if is_token_request:
                        raise ResumptionTokenFailedError(
                            "A resumption token request failed with a "
                            "transport error; the token may have been "
                            "consumed. Restart the list operation to recover."
                        ) from error
                    raise
                self._backoff_before_retry(verb, error, attempt, is_token_request)

    def _transient_retry_limit(self, is_token_request: bool) -> int:
        if is_token_request:
            return min(TOKEN_REQUEST_TRANSIENT_RETRIES, self.max_transient_retries)
        return self.max_transient_retries

    def _backoff_before_retry(
        self, verb: str, error: Exception, attempt: int, is_token_request: bool
    ) -> None:
        delay = min(
            self.request_backoff_factor * (2 ** (attempt - 1)),
            self.request_max_backoff,
        )
        logger.warning(
            "Transient failure on %s request: %r (retry %s/%s, token request: %s). "
            "Retrying in %.2fs...",
            verb,
            error,
            attempt,
            self._transient_retry_limit(is_token_request),
            is_token_request,
            delay,
        )
        if delay > 0:
            time.sleep(delay)

    def _send_with_retries(self, request: httpx.Request) -> httpx.Response:
        attempt = 0
        while True:
            attempt += 1
            try:
                return self._client.send(request)
            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.WriteTimeout):
                if attempt >= self.max_request_retries:
                    raise
                delay = min(
                    self.request_backoff_factor * (2 ** (attempt - 1)),
                    self.request_max_backoff,
                )
                logger.warning(
                    "Request timeout (attempt %s/%s). Retrying in %.2fs...",
                    attempt,
                    self.max_request_retries,
                    delay,
                )
                if delay > 0:
                    time.sleep(delay)

    def identify(self) -> Identify:
        """
        Performs the Identify request and returns a parsed Identify object.
        """
        xml = self._request("Identify")
        identify_element = xml.find("oai:Identify", namespaces=NS)
        if identify_element is None:
            raise OAIError("Invalid response: missing Identify element")
        return Identify.from_xml(identify_element)

    def list_metadata_formats(
        self, identifier: str | None = None
    ) -> Iterator[MetadataFormat]:
        """
        Performs the ListMetadataFormats request and yields MetadataFormat objects.

        :param identifier: An optional identifier to retrieve formats for a specific item.
        """
        params: dict[str, str] = {}
        if identifier:
            params["identifier"] = identifier
        xml = self._request("ListMetadataFormats", **params)
        for element in xml.findall(".//oai:metadataFormat", namespaces=NS):
            yield MetadataFormat.from_xml(element)

    def list_sets(self) -> Iterator[Set]:
        """
        Performs the ListSets request, handles resumption tokens, and yields Set objects.
        """
        params: dict[str, str] = {}
        verb = "ListSets"
        while True:
            xml = self._request(verb, **params)
            for element in xml.findall(".//oai:set", namespaces=NS):
                yield Set.from_xml(element)

            token_element = xml.find(".//oai:resumptionToken", namespaces=NS)
            if token_element is None or not token_element.text:
                break

            token = ResumptionToken.from_xml(token_element)
            params = {"resumptionToken": token.value}

    def get_record(self, identifier: str, metadata_prefix: str) -> Record:
        """
        Performs the GetRecord request and returns a Record object.

        :param identifier: The identifier of the item.
        :param metadata_prefix: The metadata prefix for the requested format.
        """
        params = {"identifier": identifier, "metadataPrefix": metadata_prefix}
        xml = self._request("GetRecord", **params)
        record_element = xml.find(".//oai:record", namespaces=NS)
        if record_element is None:
            raise OAIError("Invalid response: missing record element")
        return Record.from_xml(record_element)

    def list_identifiers(
        self,
        metadata_prefix: str,
        from_date: Datestamp | None = None,
        until_date: Datestamp | None = None,
        set_spec: str | None = None,
    ) -> Iterator[Header]:
        """
        Performs the ListIdentifiers request, handles resumption tokens, and yields Header objects.

        :param metadata_prefix: The metadata prefix for the requested format.
        :param from_date: An optional start date for selective harvesting.
        :param until_date: An optional end date for selective harvesting.
        :param set_spec: An optional set specification for selective harvesting.
        """
        granularity = self._determine_granularity_for_dates([from_date, until_date])
        params: dict[str, str | None] = {
            "metadataPrefix": metadata_prefix,
            "from": self._format_datestamp(from_date, granularity)
            if from_date
            else None,
            "until": self._format_datestamp(until_date, granularity)
            if until_date
            else None,
            "set": set_spec,
        }
        verb = "ListIdentifiers"

        while True:
            xml = self._request(verb, **params)
            for element in xml.findall(".//oai:header", namespaces=NS):
                yield Header.from_xml(element)

            token_element = xml.find(".//oai:resumptionToken", namespaces=NS)
            if token_element is None or not token_element.text:
                break

            token = ResumptionToken.from_xml(token_element)
            # When using a resumption token, the original parameters must be omitted
            params = {"resumptionToken": token.value}

    def list_records(
        self,
        metadata_prefix: str,
        from_date: Datestamp | None = None,
        until_date: Datestamp | None = None,
        set_spec: str | None = None,
    ) -> Iterator[Record]:
        """
        Performs the ListRecords request, handles resumption tokens, and yields Record objects.

        :param metadata_prefix: The metadata prefix for the requested format.
        :param from_date: An optional start date for selective harvesting.
        :param until_date: An optional end date for selective harvesting.
        :param set_spec: An optional set specification for selective harvesting.
        """
        granularity = self._determine_granularity_for_dates([from_date, until_date])
        params: dict[str, str | None] = {
            "metadataPrefix": metadata_prefix,
            "from": self._format_datestamp(from_date, granularity)
            if from_date
            else None,
            "until": self._format_datestamp(until_date, granularity)
            if until_date
            else None,
            "set": set_spec,
        }
        verb = "ListRecords"

        while True:
            xml = self._request(verb, **params)
            for element in xml.findall("./oai:ListRecords/oai:record", namespaces=NS):
                yield Record.from_xml(element)

            token_element = xml.find(".//oai:resumptionToken", namespaces=NS)
            if token_element is None or not token_element.text:
                break

            token = ResumptionToken.from_xml(token_element)
            # When using a resumption token, the original parameters must be omitted
            params = {"resumptionToken": token.value}
