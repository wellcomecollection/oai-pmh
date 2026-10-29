class OAIError(Exception):
    """Base class for OAI-PMH errors."""


class BadArgumentError(OAIError):
    """The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax."""


class BadResumptionTokenError(OAIError):
    """The value of the resumptionToken argument is invalid or expired."""


class BadVerbError(OAIError):
    """Value of the verb argument is not a legal OAI-PMH verb, the verb argument is missing, or the verb argument is repeated."""


class CannotDisseminateFormatError(OAIError):
    """The metadata format identified by the value given for the metadataPrefix argument is not supported by the item or by the repository."""


class IdDoesNotExistError(OAIError):
    """The value of the identifier argument is unknown or illegal in this repository."""


class NoRecordsMatchError(OAIError):
    """The combination of the values of the from, until, set and metadataPrefix arguments results in an empty list."""


class NoMetadataFormatsError(OAIError):
    """There are no metadata formats available for the specified item."""


class NoSetHierarchyError(OAIError):
    """The repository does not support sets."""


class ResumptionTokenFailedError(OAIError):
    """A request carrying a resumption token failed in a way that may have consumed the token.

    Many OAI-PMH servers issue single-use, session-bound resumption tokens:
    once the server has processed a request for a token, the token cannot be
    used again, even if the response was empty or lost in transit. Retrying
    the same token request therefore cannot succeed. Callers should catch
    this error and restart the list operation from the beginning.

    This is raised by the client itself, unlike BadResumptionTokenError,
    which is reported by the server through an OAI-PMH error element.
    """
