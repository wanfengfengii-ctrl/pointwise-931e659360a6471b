"""SDK2 raw event port. No response-hop association or archive adapter."""
from dataclasses import dataclass


class PortError(ValueError):
    pass


@dataclass(frozen=True)
class Response:
    session_id: str
    request_id: str
    url: str
    status: int
    headers: tuple
    expects_extra: bool


@dataclass(frozen=True)
class ExtraHeaders:
    session_id: str
    request_id: str
    headers: tuple


@dataclass(frozen=True)
class BodyChunk:
    session_id: str
    request_id: str
    data: bytes


@dataclass(frozen=True)
class BorrowedBody:
    session_id: str
    request_id: str
    data: memoryview


@dataclass(frozen=True)
class Finished:
    session_id: str
    request_id: str


@dataclass(frozen=True)
class Failed:
    session_id: str
    request_id: str
    reason: str


class Player:
    """Replay a finite valid SDK2 trace verbatim into a callback.

    Response events for a key are in hop order. ExtraHeaders events for that
    key are in the order of hops that declare expects_extra=True. The two
    sequences may be interleaved arbitrarily, including extras before response.
    Finished/Failed ends network activity; declared extras may still follow.
    BodyChunk events concatenate into the final hop body, including a single
    empty chunk for a present empty body. Failure traces have no body chunks.
    Each key has exactly one Finished or Failed. Session is part of the key.
    A successful valid trace contains every declared extra and a body.
    The raw port deliberately does not check cross-event completeness.

    BodyChunk is fixture input only. Callbacks receive BorrowedBody backed by
    a single reused buffer. It is overwritten as soon as the callback returns.
    A consumer must copy it inside the callback to retain content.
    """
    def __init__(self, events):
        self.events = tuple(events)
        allowed = (Response, ExtraHeaders, BodyChunk, Finished, Failed)
        for event in self.events:
            if not isinstance(event, allowed):
                raise PortError('unknown event type')
            if not event.session_id or not event.request_id:
                raise PortError('empty event key')
            if isinstance(event, BodyChunk) and not isinstance(event.data, bytes):
                raise PortError('fixture body must be bytes')
        self._buffer = bytearray(max(
            (len(e.data) for e in self.events if isinstance(e, BodyChunk)),
            default=0))

    def play(self, callback):
        for event in self.events:
            if not isinstance(event, BodyChunk):
                callback(event)
                continue
            size = len(event.data)
            self._buffer[:size] = event.data
            view = memoryview(self._buffer)[:size]
            try:
                callback(BorrowedBody(event.session_id, event.request_id, view))
            finally:
                self._buffer[:] = b'\xa5' * len(self._buffer)


def from_records(records):
    """Decode finite, explicit port fixtures, without correlating event keys."""
    events = []
    try:
        for row in records:
            key = row['session_id'], row['request_id']
            kind = row['kind']
            if kind == 'response':
                events.append(Response(*key, row['url'], row['status'],
                    tuple(tuple(p) for p in row['headers']), row['expects_extra']))
            elif kind == 'extra':
                events.append(ExtraHeaders(*key, tuple(tuple(p) for p in row['headers'])))
            elif kind == 'body':
                events.append(BodyChunk(*key, bytes.fromhex(row['hex'])))
            elif kind == 'finished':
                events.append(Finished(*key))
            elif kind == 'failed':
                events.append(Failed(*key, row['reason']))
            else:
                raise PortError('unknown event kind')
    except (KeyError, TypeError, ValueError) as exc:
        raise PortError('invalid event fixture') from exc
    return Player(events)
