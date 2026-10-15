"""Common base for the service APIs (`ArtifactsApi`, `ConnectorApi`) of a `Connection`.

A service API holds a reference back to its host connection while the connection caches the
service API, a reference cycle. `Connection.close()` breaks it by calling `_close()`; afterwards any
service API method that needs the connection raises `InterfaceError` instead of an `AttributeError`.
"""

from __future__ import annotations

from typing import Generic, TypeVar

from .exceptions import InterfaceError

ContextT = TypeVar("ContextT")


class ServiceApi(Generic[ContextT]):
    """Holds the host connection (typed by the service API's narrow context protocol)."""

    _context: ContextT | None

    def __init__(self, context: ContextT) -> None:
        self._context = context

    def get_context(self) -> ContextT:
        """The host connection. Raises `InterfaceError` once the connection is closed."""
        if self._context is None:
            raise InterfaceError("Connection is closed")
        return self._context

    def _close(self) -> None:
        """Drop the reference to the host connection; called by `Connection.close()`."""
        self._context = None
