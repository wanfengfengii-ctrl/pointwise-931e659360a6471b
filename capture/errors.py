class CaptureError(Exception):
    """Base error for local capture operations."""


class InvalidInput(CaptureError, ValueError):
    pass


class Conflict(CaptureError):
    pass


class NotFound(CaptureError):
    pass


class InvalidState(CaptureError):
    pass


class IntegrityError(CaptureError):
    pass


class ProtocolError(CaptureError):
    pass


class InjectedFailure(CaptureError):
    pass
