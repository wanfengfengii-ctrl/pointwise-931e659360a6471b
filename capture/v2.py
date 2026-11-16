"""SDK2 separated-event capture plus the v1 -> v2 archive migration.

Legacy archive tables stay the canonical, queryable store.  Chains assembled
from SDK2 events are published through the same ``Archive``/``Bodies``/``Jobs``
tables as SDK1 chains, so every legacy query keeps working for old history and
for newly captured chains alike.

Version 2 only adds durable event-staging tables.  ``(session_id, request_id)``
is the chain key inside a stream; responses are numbered per key in Response
arrival order.  ExtraHeaders are staged in arrival order and assigned only at
seal time, in order, to hops that declare ``expects_extra``.  Body chunks are
copied out of the borrowed buffer immediately.

Interruption model: every event is applied, any chains whose evidence became
complete are published, and the stream cursor is advanced in one SQLite
transaction.  Replaying the same event sequence after a reopen resumes at the
saved cursor, and a chain published in an earlier transaction is never
republished.
"""
import hashlib
import json

from vendor import sdk1, sdk2

from .acquisition import _complete, _headers
from .archive import Archive
from .bodies import Bodies
from .errors import Conflict, InvalidInput, InvalidState, ProtocolError
from .jobs import Jobs, validate_url

V2_TABLES = (
    'v2_streams', 'v2_staged_chains', 'v2_staged_responses',
    'v2_staged_extras', 'v2_staged_bodies',
)

V2_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS v2_streams (
        stream_id TEXT PRIMARY KEY,
        event_count INTEGER NOT NULL CHECK(event_count >= 0),
        cursor INTEGER NOT NULL CHECK(cursor >= 0 AND cursor <= event_count))""",
    """CREATE TABLE IF NOT EXISTS v2_staged_chains (
        stream_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        finished INTEGER NOT NULL DEFAULT 0 CHECK(finished IN (0,1)),
        failed INTEGER NOT NULL DEFAULT 0 CHECK(failed IN (0,1)),
        failure TEXT,
        body_chunks INTEGER NOT NULL DEFAULT 0 CHECK(body_chunks >= 0),
        sealed INTEGER NOT NULL DEFAULT 0 CHECK(sealed IN (0,1)),
        chain_id TEXT,
        PRIMARY KEY(stream_id,session_id,request_id),
        CHECK(NOT (finished = 1 AND failed = 1)),
        CHECK(failed = 0 OR failure IS NOT NULL))""",
    """CREATE TABLE IF NOT EXISTS v2_staged_responses (
        stream_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        position INTEGER NOT NULL CHECK(position >= 0),
        url TEXT NOT NULL,
        status INTEGER NOT NULL,
        headers_json TEXT NOT NULL,
        expects_extra INTEGER NOT NULL CHECK(expects_extra IN (0,1)),
        PRIMARY KEY(stream_id,session_id,request_id,position))""",
    """CREATE TABLE IF NOT EXISTS v2_staged_extras (
        stream_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        position INTEGER NOT NULL CHECK(position >= 0),
        headers_json TEXT NOT NULL,
        PRIMARY KEY(stream_id,session_id,request_id,position))""",
    """CREATE TABLE IF NOT EXISTS v2_staged_bodies (
        stream_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        position INTEGER NOT NULL CHECK(position >= 0),
        payload BLOB NOT NULL,
        size INTEGER NOT NULL CHECK(size >= 0),
        sha256 TEXT NOT NULL,
        PRIMARY KEY(stream_id,session_id,request_id,position))""",
)


class CaptureV2Result(list):
    """Newly sealed chains for one call, with stream cursor metadata.

    List semantics match ``capture(...)``; entries are ``{'chain_id','state'}``.
    ``cursor`` is the number of events already durably applied to this stream.
    A sealed chain is never included in a later call's list.
    """
    def __init__(self, chains, stream_id, cursor, event_count):
        super().__init__(chains)
        self.stream_id = stream_id
        self.cursor = cursor
        self.event_count = event_count

    @property
    def complete(self):
        return self.cursor >= self.event_count

    @property
    def chains(self):
        return list(self)

    def __repr__(self):
        return (f'CaptureV2Result({list.__repr__(self)}, stream_id={self.stream_id!r}, '
                f'cursor={self.cursor}, event_count={self.event_count})')


def upgrade(db):
    """Idempotently migrate an existing v1 database to storage version 2.

    The v2 schema additions and the ``meta.storage_version`` bump commit in a
    single transaction.  Existing v1 rows are never rewritten.  An injected
    failure (including at ``migration.after`` or ``before_commit``) rolls the
    whole upgrade back, so a failed upgrade never exposes a half-migrated
    database.  Calling it again on a v2 database is a no-op.
    """
    row = db.connection.execute(
        "SELECT value FROM meta WHERE key='storage_version'").fetchone()
    if row is None or row[0] not in ('1', '2'):
        raise InvalidState('unsupported storage version')
    if row[0] == '2':
        return 2
    with db.atomic() as conn:
        for statement in V2_SCHEMA:
            conn.execute(statement)
        conn.execute("UPDATE meta SET value='2' WHERE key='storage_version'")
        db.trip('migration.after')
    return 2


def _headers_json(headers):
    """Validate an ordered pair sequence the way SDK1 evidence is validated."""
    return json.dumps([list(pair) for pair in _headers(headers)],
                      ensure_ascii=False)


def _load_headers(text):
    return tuple(tuple(pair) for pair in json.loads(text))


def _encode_event(event):
    """Canonical, deterministic encoding used to identify a replay stream."""
    if isinstance(event, sdk2.Response):
        return ['response', event.session_id, event.request_id, event.url,
                event.status, [list(p) for p in event.headers],
                bool(event.expects_extra)]
    if isinstance(event, sdk2.ExtraHeaders):
        return ['extra', event.session_id, event.request_id,
                [list(p) for p in event.headers]]
    if isinstance(event, sdk2.BodyChunk):
        return ['body', event.session_id, event.request_id,
                bytes(event.data).hex()]
    if isinstance(event, sdk2.BorrowedBody):
        return ['body', event.session_id, event.request_id,
                bytes(event.data).hex()]
    if isinstance(event, sdk2.Finished):
        return ['finished', event.session_id, event.request_id]
    if isinstance(event, sdk2.Failed):
        return ['failed', event.session_id, event.request_id, event.reason]
    raise ProtocolError('unsupported SDK2 event type')


def stream_id(events):
    """Stable identity of one finite event sequence across reopen/replay."""
    try:
        encoded = json.dumps([_encode_event(e) for e in events],
                             ensure_ascii=False, separators=(',', ':'))
    except (TypeError, ValueError) as exc:
        raise ProtocolError('unencodable SDK2 event') from exc
    return hashlib.sha256(encoded.encode()).hexdigest()


def _request_key(sid, session_id, request_id):
    # JSON keeps the three components unambiguous even when ids contain ':'.
    return 'v2:' + sid + ':' + json.dumps(
        [session_id, request_id], ensure_ascii=False, separators=(',', ':'))


def _fallback_url(sid, session_id, request_id):
    token = hashlib.sha256(
        f'{sid}\0{session_id}\0{request_id}'.encode()).hexdigest()[:24]
    return f'https://sdk2.invalid/{token}'


def _chain_row(conn, sid, session_id, request_id):
    row = conn.execute(
        """SELECT stream_id,session_id,request_id,finished,failed,failure,
                  body_chunks,sealed,chain_id
           FROM v2_staged_chains
           WHERE stream_id=? AND session_id=? AND request_id=?""",
        (sid, session_id, request_id)).fetchone()
    if row is not None:
        return row
    conn.execute(
        """INSERT INTO v2_staged_chains(stream_id,session_id,request_id)
           VALUES (?,?,?)""", (sid, session_id, request_id))
    return conn.execute(
        """SELECT stream_id,session_id,request_id,finished,failed,failure,
                  body_chunks,sealed,chain_id
           FROM v2_staged_chains
           WHERE stream_id=? AND session_id=? AND request_id=?""",
        (sid, session_id, request_id)).fetchone()


def _next_position(conn, table, sid, session_id, request_id):
    return conn.execute(
        f'SELECT COALESCE(MAX(position),-1)+1 FROM {table} '
        'WHERE stream_id=? AND session_id=? AND request_id=?',
        (sid, session_id, request_id)).fetchone()[0]


def _apply(db, conn, sid, event):
    """Stage one event.  Raises ProtocolError for structurally invalid input."""
    session_id = getattr(event, 'session_id', None)
    request_id = getattr(event, 'request_id', None)
    if not isinstance(session_id, str) or not session_id or \
            not isinstance(request_id, str) or not request_id:
        raise ProtocolError('empty event key')

    if isinstance(event, sdk2.Response):
        try:
            validate_url(event.url)
        except InvalidInput as exc:
            raise ProtocolError('invalid response URL') from exc
        if type(event.status) is not int or not 200 <= event.status <= 599:
            raise ProtocolError('invalid response status')
        row = _chain_row(conn, sid, session_id, request_id)
        if row['sealed']:
            raise ProtocolError('response after chain was sealed')
        if row['finished'] or row['failed']:
            raise ProtocolError('response after network end')
        _headers_json(event.headers)  # validate before staging
        conn.execute(
            """INSERT INTO v2_staged_responses
               (stream_id,session_id,request_id,position,url,status,
                headers_json,expects_extra) VALUES (?,?,?,?,?,?,?,?)""",
            (sid, session_id, request_id,
             _next_position(conn, 'v2_staged_responses',
                            sid, session_id, request_id),
             event.url, event.status, _headers_json(event.headers),
             1 if event.expects_extra else 0))
        return

    if isinstance(event, sdk2.ExtraHeaders):
        row = _chain_row(conn, sid, session_id, request_id)
        if row['sealed']:
            raise ProtocolError('extra headers after chain was sealed')
        conn.execute(
            """INSERT INTO v2_staged_extras
               (stream_id,session_id,request_id,position,headers_json)
               VALUES (?,?,?,?,?)""",
            (sid, session_id, request_id,
             _next_position(conn, 'v2_staged_extras',
                            sid, session_id, request_id),
             _headers_json(event.headers)))
        return

    if isinstance(event, (sdk2.BorrowedBody, sdk2.BodyChunk)):
        row = _chain_row(conn, sid, session_id, request_id)
        if row['sealed']:
            raise ProtocolError('body after chain was sealed')
        if row['finished']:
            raise ProtocolError('body after network end')
        if row['failed']:
            raise ProtocolError('body on a failed chain')
        # The raw port reuses one buffer as soon as this callback returns.
        payload = bytes(event.data)
        position = _next_position(conn, 'v2_staged_bodies',
                                  sid, session_id, request_id)
        conn.execute(
            """INSERT INTO v2_staged_bodies
               (stream_id,session_id,request_id,position,payload,size,sha256)
               VALUES (?,?,?,?,?,?,?)""",
            (sid, session_id, request_id, position, payload, len(payload),
             hashlib.sha256(payload).hexdigest()))
        conn.execute(
            """UPDATE v2_staged_chains SET body_chunks=body_chunks+1
               WHERE stream_id=? AND session_id=? AND request_id=?""",
            (sid, session_id, request_id))
        return

    if isinstance(event, sdk2.Finished):
        row = _chain_row(conn, sid, session_id, request_id)
        if row['sealed']:
            raise ProtocolError('duplicate network end')
        if row['finished'] or row['failed']:
            raise ProtocolError('duplicate network end')
        conn.execute(
            """UPDATE v2_staged_chains SET finished=1
               WHERE stream_id=? AND session_id=? AND request_id=?""",
            (sid, session_id, request_id))
        return

    if isinstance(event, sdk2.Failed):
        row = _chain_row(conn, sid, session_id, request_id)
        if row['sealed']:
            raise ProtocolError('duplicate network end')
        if row['finished'] or row['failed']:
            raise ProtocolError('duplicate network end')
        if not isinstance(event.reason, str) or not event.reason.strip():
            raise ProtocolError('failure requires a nonempty reason')
        conn.execute(
            """UPDATE v2_staged_chains SET failed=1,failure=?
               WHERE stream_id=? AND session_id=? AND request_id=?""",
            (event.reason, sid, session_id, request_id))
        return

    raise ProtocolError('unsupported SDK2 event type')


def _existing_chain(conn, job_id):
    return conn.execute(
        'SELECT state,chain_id FROM attempts WHERE job_id=? AND number=1',
        (job_id,)).fetchone()


def _ensure_job(conn, request_key, url):
    """Return (job_id, attempt_row).  A reused terminal job is never republished."""
    old = conn.execute(
        'SELECT job_id,url FROM jobs WHERE request_key=?',
        (request_key,)).fetchone()
    if old is not None:
        if old['url'] != url:
            raise Conflict('request_key is already bound to another URL')
        attempt = _existing_chain(conn, old['job_id'])
        if attempt is None:
            conn.execute("INSERT INTO attempts VALUES (?,1,'pending',NULL)",
                         (old['job_id'],))
            attempt = _existing_chain(conn, old['job_id'])
        return old['job_id'], attempt
    cur = conn.execute(
        "INSERT INTO jobs(request_key,url,state) VALUES (?,?,'pending')",
        (request_key, url))
    job_id = cur.lastrowid
    conn.execute("INSERT INTO attempts VALUES (?,1,'pending',NULL)", (job_id,))
    return job_id, _existing_chain(conn, job_id)


def _seal_ready(db, conn, jobs, archive, bodies, sid, sealed):
    """Publish every chain whose terminal event and declared evidence are all in.

    Network end alone is not completeness: a chain stays staged until every
    hop declaring ``expects_extra`` has received its ordered ExtraHeaders
    event (and, for success, at least one body chunk has arrived).  Failure
    chains retain confirmed responses and never gain a body.
    """
    rows = conn.execute(
        """SELECT * FROM v2_staged_chains
           WHERE stream_id=? AND sealed=0
           ORDER BY rowid""", (sid,)).fetchall()
    for row in rows:
        session_id = row['session_id']
        request_id = row['request_id']
        if not (row['finished'] or row['failed']):
            continue
        responses = conn.execute(
            """SELECT position,url,status,headers_json,expects_extra
               FROM v2_staged_responses
               WHERE stream_id=? AND session_id=? AND request_id=?
               ORDER BY position""",
            (sid, session_id, request_id)).fetchall()
        extras = conn.execute(
            """SELECT position,headers_json FROM v2_staged_extras
               WHERE stream_id=? AND session_id=? AND request_id=?
               ORDER BY position""",
            (sid, session_id, request_id)).fetchall()
        expected = [response for response in responses
                    if response['expects_extra']]
        if len(extras) < len(expected):
            continue  # Network ended, but declared extra headers have not.
        if len(extras) > len(expected):
            raise ProtocolError('extra headers have no declared slot')

        if row['failed']:
            if row['body_chunks']:
                raise ProtocolError('body on a failed chain')
            body_bytes = None
            failure = row['failure']
        else:
            if row['body_chunks'] == 0:
                continue  # Success needs the final hop's body.
            chunks = conn.execute(
                """SELECT payload FROM v2_staged_bodies
                   WHERE stream_id=? AND session_id=? AND request_id=?
                   ORDER BY position""",
                (sid, session_id, request_id)).fetchall()
            body_bytes = b''.join(bytes(chunk['payload']) for chunk in chunks)
            failure = None

        extra_index = 0
        hops = []
        for response in responses:
            extra_headers = None
            if response['expects_extra']:
                extra_headers = _load_headers(
                    extras[extra_index]['headers_json'])
                extra_index += 1
            hops.append(sdk1.Hop(
                response['url'], response['status'],
                _load_headers(response['headers_json']), extra_headers))
        target_url = hops[0].url if hops else _fallback_url(
            sid, session_id, request_id)
        raw = sdk1.CompleteChain(session_id, request_id, tuple(hops),
                                 body_bytes, failure)
        result = _complete(raw, target_url)

        request_key = _request_key(sid, session_id, request_id)
        job_id, attempt = _ensure_job(conn, request_key, target_url)
        if attempt is not None and attempt['chain_id'] is not None:
            # Defensive idempotency: already durable, never republish.
            conn.execute(
                """UPDATE v2_staged_chains SET sealed=1,chain_id=?
                   WHERE stream_id=? AND session_id=? AND request_id=?""",
                (attempt['chain_id'], sid, session_id, request_id))
            continue

        db.trip('v2.seal.before')
        body = None
        if result.failure is None:
            body = bodies.save(conn, f'b{job_id}.1', result.body)
        chain_id, state = archive.publish(conn, job_id, 1, result, body)
        jobs.finish(conn, job_id, 1, chain_id, state)
        conn.execute(
            """UPDATE v2_staged_chains SET sealed=1,chain_id=?
               WHERE stream_id=? AND session_id=? AND request_id=?""",
            (chain_id, sid, session_id, request_id))
        db.trip('v2.seal.after')
        sealed.append({'chain_id': chain_id, 'state': state})


class _StopPlayback(Exception):
    pass


def capture_v2(player, db, limit=None):
    """Capture one finite SDK2 event sequence into legacy archive tables.

    ``player`` can be replayed from the beginning any number of times.  The
    first call records the sequence identity and a durable per-event cursor;
    later calls skip already-applied events and only publish chains that have
    newly become complete.  ``limit`` caps how many not-yet-applied events this
    call consumes (``None`` consumes all), which is how an interrupted stream
    is resumed in bounded segments.  A chain is published at most once.

    Returns a list-like :class:`CaptureV2Result` of newly sealed chains, each
    ``{'chain_id', 'state'}``, with ``stream_id``, ``cursor``, ``event_count``
    and ``complete`` metadata.  Structural protocol errors raise
    ``ProtocolError`` and roll back only the event being applied; already
    committed events and chains remain durable.
    """
    if limit is not None and (type(limit) is not int or limit < 0):
        raise InvalidInput('limit must be a nonnegative integer or None')
    play = getattr(player, 'play', None)
    events = getattr(player, 'events', None)
    if not callable(play) or not isinstance(events, (tuple, list)):
        raise ProtocolError('player must expose events and play(callback)')
    events = tuple(events)
    upgrade(db)
    sid = stream_id(events)

    with db.atomic() as conn:
        row = conn.execute(
            'SELECT event_count,cursor FROM v2_streams WHERE stream_id=?',
            (sid,)).fetchone()
        if row is None:
            conn.execute('INSERT INTO v2_streams VALUES (?,?,0)',
                         (sid, len(events)))
            cursor = 0
        else:
            if row['event_count'] != len(events):
                raise Conflict('stream sequence changed after interruption')
            cursor = row['cursor']

    jobs = Jobs(db)
    archive = Archive(db)
    bodies = Bodies(db)
    sealed = []
    index = -1
    processed = 0

    def accept(event):
        nonlocal index, cursor, processed
        index += 1
        if index < cursor:
            return
        if limit is not None and processed >= limit:
            raise _StopPlayback()
        local = []
        with db.atomic() as conn:
            db.trip('v2.event.before')
            _apply(db, conn, sid, event)
            _seal_ready(db, conn, jobs, archive, bodies, sid, local)
            conn.execute(
                'UPDATE v2_streams SET cursor=? WHERE stream_id=?',
                (index + 1, sid))
            db.trip('v2.event.after')
        cursor = index + 1
        processed += 1
        sealed.extend(local)

    try:
        player.play(accept)
    except _StopPlayback:
        pass
    return CaptureV2Result(sealed, sid, cursor, len(events))


def stream_state(db, sid):
    """Durable cursor view for one stream, or None for an unknown stream."""
    exists = db.connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='v2_streams'"
    ).fetchone()
    if exists is None:
        return None
    row = db.connection.execute(
        'SELECT event_count,cursor FROM v2_streams WHERE stream_id=?',
        (sid,)).fetchone()
    if row is None:
        return None
    return {'stream_id': sid, 'event_count': row['event_count'],
            'cursor': row['cursor'], 'complete': row['cursor'] >= row['event_count']}


def stream_chains(db, sid):
    """Every chain id this stream has sealed, in seal order (None if unknown)."""
    if stream_state(db, sid) is None:
        return None
    return [row[0] for row in db.connection.execute(
        """SELECT chain_id FROM v2_staged_chains
           WHERE stream_id=? AND sealed=1 ORDER BY rowid""", (sid,))]
