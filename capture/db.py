import sqlite3
from contextlib import contextmanager
from .errors import InjectedFailure, InvalidState

SCHEMA = (
    "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    """CREATE TABLE jobs (
        job_id INTEGER PRIMARY KEY, request_key TEXT NOT NULL UNIQUE,
        url TEXT NOT NULL, state TEXT NOT NULL
        CHECK(state IN ('pending','succeeded','failed')))""",
    """CREATE TABLE attempts (
        job_id INTEGER NOT NULL REFERENCES jobs(job_id),
        number INTEGER NOT NULL CHECK(number > 0),
        state TEXT NOT NULL CHECK(state IN ('pending','succeeded','failed')),
        chain_id TEXT UNIQUE REFERENCES chains_v1(chain_id),
        PRIMARY KEY(job_id,number),
        CHECK((state='pending' AND chain_id IS NULL) OR
              (state!='pending' AND chain_id IS NOT NULL)))""",
    """CREATE TABLE bodies_v1 (
        body_ref TEXT PRIMARY KEY, payload BLOB NOT NULL,
        size INTEGER NOT NULL CHECK(size >= 0), sha256 TEXT NOT NULL)""",
    """CREATE TABLE chains_v1 (
        chain_id TEXT PRIMARY KEY,
        job_id INTEGER NOT NULL, attempt INTEGER NOT NULL,
        session_id TEXT NOT NULL, request_id TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('succeeded','failed')),
        failure TEXT, body_ref TEXT REFERENCES bodies_v1(body_ref),
        body_position INTEGER,
        UNIQUE(job_id,attempt),
        FOREIGN KEY(job_id,attempt) REFERENCES attempts(job_id,number),
        CHECK((state='succeeded' AND failure IS NULL AND
               body_ref IS NOT NULL AND body_position IS NOT NULL) OR
              (state='failed' AND failure IS NOT NULL AND
               body_ref IS NULL AND body_position IS NULL)))""",
    """CREATE TABLE hops_v1 (
        chain_id TEXT NOT NULL REFERENCES chains_v1(chain_id),
        position INTEGER NOT NULL CHECK(position >= 0),
        url TEXT NOT NULL, status INTEGER NOT NULL,
        headers_json TEXT NOT NULL, extra_json TEXT,
        PRIMARY KEY(chain_id,position))""",
    """CREATE TABLE receipts_v1 (
        receipt_id TEXT PRIMARY KEY,
        chain_id TEXT NOT NULL UNIQUE REFERENCES chains_v1(chain_id),
        payload_json TEXT NOT NULL)""",
)


class FaultPlan:
    """Deterministic, one-shot faults at actual storage boundaries."""
    def __init__(self):
        self._armed = {}

    def arm(self, stage, after=0):
        if stage not in {'body.before', 'body.after', 'archive.after',
                         'jobs.after', 'before_commit'}:
            raise ValueError('unknown fault stage')
        if type(after) is not int or after < 0:
            raise ValueError('after must be a nonnegative integer')
        self._armed[stage] = after

    def __call__(self, stage):
        if stage not in self._armed:
            return
        if self._armed[stage]:
            self._armed[stage] -= 1
            return
        del self._armed[stage]
        raise InjectedFailure(stage)


class Database:
    """One maintenance-window worker; no nested or concurrent operations."""
    def __init__(self, path=':memory:', fault=None):
        self.connection = sqlite3.connect(str(path), isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.fault = fault
        try:
            exists = self.connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'"
            ).fetchone()
            if exists is None:
                with self.atomic(checkpoint=False):
                    for statement in SCHEMA:
                        self.connection.execute(statement)
                    self.connection.execute(
                        "INSERT INTO meta VALUES ('storage_version','1')")
            version = self.connection.execute(
                "SELECT value FROM meta WHERE key='storage_version'").fetchone()
            if version is None or version[0] != '1':
                raise InvalidState('unsupported storage version')
        except BaseException:
            self.connection.close()
            raise

    def trip(self, stage):
        if self.fault is not None:
            self.fault(stage)

    @contextmanager
    def atomic(self, checkpoint=True):
        if self.connection.in_transaction:
            raise InvalidState('nested transaction')
        self.connection.execute('BEGIN IMMEDIATE')
        try:
            yield self.connection
            if checkpoint:
                self.trip('before_commit')
            self.connection.execute('COMMIT')
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute('ROLLBACK')
            raise

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
