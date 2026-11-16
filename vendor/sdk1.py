"""SDK1: each fetch transfers an already complete response chain."""
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class Target:
    url: str
    request_key: str


@dataclass(frozen=True)
class Hop:
    url: str
    status: int
    headers: tuple
    extra_headers: tuple | None = None


@dataclass(frozen=True)
class CompleteChain:
    session_id: str
    request_id: str
    hops: tuple
    body: bytes | None
    failure: str | None = None


class NetworkFailure(Exception):
    def __init__(self, session_id, request_id, hops, reason):
        super().__init__(reason)
        self.session_id = session_id
        self.request_id = request_id
        self.hops = tuple(hops)
        self.reason = reason


class SourceExhausted(Exception):
    pass


class Source:
    """Finite SDK1 acquisition source. One scripted outcome per actual fetch.

    Data is supplied directly, without consulting SDK2 or combining its events.
    NetworkFailure outcomes raise with confirmed hops. Exhaustion is a port
    error, not a network result and not an archived failed capture.
    """
    def __init__(self, targets, outcomes):
        self.targets = tuple(targets)
        self._outcomes = {url: deque(values) for url, values in outcomes.items()}
        self.calls = []

    def fetch(self, url):
        self.calls.append(url)
        if url not in self._outcomes or not self._outcomes[url]:
            raise SourceExhausted(url)
        result = self._outcomes[url].popleft()
        if isinstance(result, NetworkFailure):
            raise result
        if not isinstance(result, CompleteChain):
            raise TypeError('SDK1 outcome must be CompleteChain or NetworkFailure')
        return result
