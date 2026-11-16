import hashlib
from .errors import IntegrityError


class Bodies:
    def __init__(self, db):
        self.db = db

    def save(self, conn, body_ref, payload):
        if not isinstance(payload, bytes):
            raise TypeError('body must be immutable bytes')
        self.db.trip('body.before')
        digest = hashlib.sha256(payload).hexdigest()
        conn.execute('INSERT INTO bodies_v1 VALUES (?,?,?,?)',
                     (body_ref, payload, len(payload), digest))
        self.db.trip('body.after')
        return {'body_ref': body_ref, 'size': len(payload), 'sha256': digest}

    def metadata(self, body_ref):
        row = self.db.connection.execute(
            'SELECT body_ref,size,sha256 FROM bodies_v1 WHERE body_ref=?',
            (body_ref,)).fetchone()
        if row is None:
            raise IntegrityError('body reference is missing')
        return dict(row)

    def read_body(self, body_ref):
        row = self.db.connection.execute(
            'SELECT payload,size,sha256 FROM bodies_v1 WHERE body_ref=?',
            (body_ref,)).fetchone()
        if row is None:
            raise IntegrityError('body reference is missing')
        payload = bytes(row['payload'])
        if (len(payload) != row['size'] or
                hashlib.sha256(payload).hexdigest() != row['sha256']):
            raise IntegrityError('body length or SHA-256 mismatch')
        return payload
