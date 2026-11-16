from urllib.parse import urlsplit
from .errors import Conflict, InvalidInput, InvalidState, NotFound


def validate_url(url):
    if not isinstance(url, str) or any(c.isspace() for c in url):
        raise InvalidInput('URL must be an HTTP(S) URL without whitespace')
    try:
        parts = urlsplit(url)
        parts.port  # Validate a supplied port as well as the hostname.
    except ValueError as exc:
        raise InvalidInput('invalid URL') from exc
    if parts.scheme not in ('http', 'https') or not parts.hostname:
        raise InvalidInput('URL must have an HTTP(S) scheme and hostname')
    if parts.username is not None or parts.password is not None:
        raise InvalidInput('URL credentials are unsupported')
    return url


class Jobs:
    def __init__(self, db):
        self.db = db

    def enqueue(self, url, request_key):
        validate_url(url)
        if not isinstance(request_key, str) or not request_key.strip():
            raise InvalidInput('request_key must be nonempty text')
        with self.db.atomic() as conn:
            old = conn.execute(
                'SELECT job_id,url FROM jobs WHERE request_key=?',
                (request_key,)).fetchone()
            if old is not None:
                if old['url'] != url:
                    raise Conflict('request_key is already bound to another URL')
                return old['job_id']
            cur = conn.execute(
                "INSERT INTO jobs(request_key,url,state) VALUES (?,?,'pending')",
                (request_key, url))
            job_id = cur.lastrowid
            conn.execute(
                "INSERT INTO attempts VALUES (?,1,'pending',NULL)", (job_id,))
            return job_id

    def get_job(self, job_id):
        conn = self.db.connection
        row = conn.execute('SELECT * FROM jobs WHERE job_id=?', (job_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result['attempts'] = [dict(r) for r in conn.execute(
            'SELECT number,state,chain_id FROM attempts WHERE job_id=? ORDER BY number',
            (job_id,))]
        return result

    def pending(self, job_id):
        job = self.get_job(job_id)
        if job is None:
            raise NotFound('unknown job')
        attempt = job['attempts'][-1]
        if job['state'] != 'pending' or attempt['state'] != 'pending':
            raise InvalidState('job has no pending attempt')
        return job['url'], attempt['number']

    def retry(self, job_id):
        with self.db.atomic() as conn:
            job = self.get_job(job_id)
            if job is None:
                raise NotFound('unknown job')
            if job['state'] != 'failed':
                raise InvalidState('only a failed job can be retried')
            number = job['attempts'][-1]['number'] + 1
            conn.execute("INSERT INTO attempts VALUES (?,?,'pending',NULL)",
                         (job_id, number))
            conn.execute("UPDATE jobs SET state='pending' WHERE job_id=?", (job_id,))
            return number

    def finish(self, conn, job_id, number, chain_id, state):
        if state not in ('succeeded', 'failed'):
            raise InvalidState('invalid terminal state')
        cur = conn.execute(
            """UPDATE attempts SET state=?,chain_id=?
               WHERE job_id=? AND number=? AND state='pending'""",
            (state, chain_id, job_id, number))
        if cur.rowcount != 1:
            raise InvalidState('attempt already finished')
        conn.execute('UPDATE jobs SET state=? WHERE job_id=?', (state, job_id))
        self.db.trip('jobs.after')
