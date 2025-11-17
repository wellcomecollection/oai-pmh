class OAIError(Exception):
    """Base class for OAI-PMH errors."""

    pass


class BadArgumentError(OAIError):
    """The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax."""

    pass


class BadResumptionTokenError(OAIError):
    """The value of the resumptionToken argument is invalid or expired."""

    pass


class BadVerbError(OAIError):
    """Value of the verb argument is not a legal OAI-PMH verb, the verb argument is missing, or the verb argument is repeated."""

    pass


class CannotDisseminateFormatError(OAIError):
    """The metadata format identified by the value given for the metadataPrefix argument is not supported by the item or by the repository."""

    pass


class IdDoesNotExistError(OAIError):
    """The value of the identifier argument is unknown or illegal in this repository."""

    pass


class NoRecordsMatchError(OAIError):
    """The combination of the values of the from, until, set and metadataPrefix arguments results in an empty list."""

    pass


class NoMetadataFormatsError(OAIError):
    """There are no metadata formats available for the specified item."""

    pass


class NoSetHierarchyError(OAIError):
    """The repository does not support sets."""

    pass
