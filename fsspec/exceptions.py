"""
fsspec user-defined exception classes
"""

import asyncio


class BlocksizeMismatchError(ValueError):
    """
    Raised when a cached file is opened with a different blocksize than it was
    written with
    """


class FSTimeoutError(asyncio.TimeoutError):
    """
    Raised when a fsspec function timed out occurs
    """


class ConsistencyError(OSError):
    """
    Base class for read-consistency errors.

    Raised by files opened in consistent-read mode (``consistency=...``),
    where the handle is bound to the version of the remote object that was
    current when it was opened.
    """


class ObjectChangedError(ConsistencyError):
    """
    The remote object changed while it was being read.

    A file opened in consistent-read mode is bound to a single object
    version. If a range fetch observes a different version, the read fails
    with this error rather than returning bytes stitched together from
    different versions.
    """


class ConsistencyNotSupportedError(ConsistencyError, NotImplementedError):
    """
    The filesystem cannot provide a verifiable object identity.

    Consistent-read mode requires the backend to expose a version token
    (e.g. a version id, generation or etag) and/or to accept a verifiable
    condition on range reads. File size and modification time alone do not
    qualify, since they cannot detect same-size overwrites.
    """
