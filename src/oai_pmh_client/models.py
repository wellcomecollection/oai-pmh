from __future__ import annotations
from datetime import datetime
from typing import Any, Iterator, Optional, Self

from lxml import etree
from pydantic import BaseModel, Field, field_validator, ConfigDict

# XML namespaces used in OAI-PMH responses
NS = {"oai": "http://www.openarchives.org/OAI/2.0/"}


def _find_all(element: etree._Element, xpath: str) -> list[etree._Element]:
    """Helper to find all elements with the namespace."""
    return element.findall(xpath, namespaces=NS)


def _find(element: etree._Element, xpath: str) -> etree._Element | None:
    """Helper to find a single element with the namespace."""
    return element.find(xpath, namespaces=NS)


def _find_text(element: etree._Element, xpath: str) -> str | None:
    """Helper to find the text content of a single element."""
    return element.findtext(xpath, namespaces=NS)


class ResumptionToken(BaseModel):
    """
    Represents a resumption token for fetching further results.
    """
    value: str = Field(..., description="The resumption token string.")
    expiration_date: Optional[datetime] = Field(
        None, alias="expirationDate", description="The expiration date of the token."
    )
    complete_list_size: Optional[int] = Field(
        None, alias="completeListSize", description="The total number of records in the list."
    )
    cursor: Optional[int] = Field(None, description="The current position in the list.")

    @classmethod
    def from_xml(cls, element: etree._Element) -> Self:
        """Parses a ResumptionToken from an XML element."""
        return cls(
            value=element.text or "",
            expirationDate=element.get("expirationDate"),
            completeListSize=element.get("completeListSize"),
            cursor=element.get("cursor"),
        )


class Header(BaseModel):
    """
    Represents the header of a record.
    """
    identifier: str = Field(..., description="The unique identifier of the item.")
    datestamp: datetime = Field(
        ..., description="The datestamp of the record."
    )
    set_specs: list[str] = Field(
        default_factory=list,
        alias="setSpec",
        description="A list of set specifications the item belongs to.",
    )
    is_deleted: bool = Field(
        False,
        alias="status",
        description="A boolean indicating if the record is deleted.",
    )

    @field_validator("is_deleted", mode="before")
    def _validate_deleted(cls, v: Any) -> bool:
        return v == "deleted"

    @classmethod
    def from_xml(cls, element: etree._Element) -> Self:
        """Parses a Header from an XML element."""
        return cls(
            identifier=_find_text(element, "oai:identifier"),
            datestamp=_find_text(element, "oai:datestamp"),
            setSpec=[spec.text for spec in _find_all(element, "oai:setSpec") if spec.text],
            status=element.get("status"),
        )


class MetadataFormat(BaseModel):
    """
    Represents a metadata format supported by the repository.
    """
    prefix: str = Field(
        ..., alias="metadataPrefix", description="The metadata prefix."
    )
    schema_location: str = Field(..., alias="schema", description="The URL of the XML schema.")
    namespace: str = Field(
        ..., alias="metadataNamespace", description="The XML namespace URI."
    )

    @classmethod
    def from_xml(cls, element: etree._Element) -> Self:
        """Parses a MetadataFormat from an XML element."""
        return cls(
            metadataPrefix=_find_text(element, "oai:metadataPrefix"),
            schema=_find_text(element, "oai:schema"),
            metadataNamespace=_find_text(element, "oai:metadataNamespace"),
        )


class Set(BaseModel):
    """
    Represents a set in the repository.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    spec: str = Field(..., alias="setSpec", description="The set specification.")
    name: str = Field(..., alias="setName", description="The name of the set.")
    description: Optional[etree._Element] = Field(
        None,
        alias="setDescription",
        description="Optional description of the set.",
    )

    @field_validator("description", mode="before")
    def _validate_description(cls, v: Any) -> Any:
        if isinstance(v, etree._Element) and len(v):
            return v
        return None

    @classmethod
    def from_xml(cls, element: etree._Element) -> Self:
        """Parses a Set from an XML element."""
        return cls(
            setSpec=_find_text(element, "oai:setSpec"),
            setName=_find_text(element, "oai:setName"),
            setDescription=_find(element, "oai:setDescription"),
        )


class Record(BaseModel):
    """
    Represents a single record.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    header: Optional[Header] = Field(None, description="The record header.")
    metadata: Optional[etree._Element] = Field(
        None, description="The metadata of the record as an XML element."
    )

    @field_validator("metadata", mode="before")
    def _validate_metadata(cls, v: Any) -> Any:
        # The metadata element contains the payload as its first and only child
        if isinstance(v, etree._Element) and len(v):
            return v[0]
        return None

    @classmethod
    def from_xml(cls, element: etree._Element) -> Self:
        """Parses a Record from an XML element."""
        header_element = _find(element, "oai:header")

        return cls(
            header=Header.from_xml(header_element) if header_element is not None else None,
            metadata=_find(element, "oai:metadata"),
        )


class Identify(BaseModel):
    """
    Represents the response from an Identify request.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    repository_name: str = Field(..., alias="repositoryName")
    base_url: str = Field(..., alias="baseURL")
    protocol_version: str = Field(..., alias="protocolVersion")
    admin_emails: list[str] = Field(..., alias="adminEmail")
    earliest_datestamp: datetime = Field(..., alias="earliestDatestamp")
    deleted_record: str = Field(..., alias="deletedRecord")
    granularity: str
    compressions: list[str] = Field(default_factory=list, alias="compression")
    descriptions: list[etree._Element] = Field(
        default_factory=list, alias="description"
    )

    @field_validator("descriptions", mode="before")
    def _validate_descriptions(cls, v: Any) -> Any:
        if isinstance(v, list) and len(v) > 0:
            return v
        return []

    @classmethod
    def from_xml(cls, element: etree._Element) -> Self:
        """Parses an Identify response from an XML element."""
        return cls(
            repositoryName=_find_text(element, "oai:repositoryName"),
            baseURL=_find_text(element, "oai:baseURL"),
            protocolVersion=_find_text(element, "oai:protocolVersion"),
            adminEmail=[
                email.text for email in _find_all(element, "oai:adminEmail") if email.text
            ],
            earliestDatestamp=_find_text(element, "oai:earliestDatestamp"),
            deletedRecord=_find_text(element, "oai:deletedRecord"),
            granularity=_find_text(element, "oai:granularity"),
            compression=[
                comp.text for comp in _find_all(element, "oai:compression") if comp.text
            ],
            description=_find_all(element, "oai:description"),
        )
