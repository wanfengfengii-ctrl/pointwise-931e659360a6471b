import json
from .bodies import Bodies


class Archive:
    """SDK1 archival writer and stable legacy query interface."""
    def __init__(self, db):
        self.db = db

    def publish(self, conn, job_id, attempt, result, body):
        chain_id = f'c{job_id}.{attempt}'
        state = 'failed' if result.failure is not None else 'succeeded'
        body_ref = None if body is None else body['body_ref']
        position = None if body is None else len(result.hops) - 1
        conn.execute(
            'INSERT INTO chains_v1 VALUES (?,?,?,?,?,?,?,?,?)',
            (chain_id, job_id, attempt, result.session_id, result.request_id,
             state, result.failure, body_ref, position))
        for number, hop in enumerate(result.hops):
            conn.execute('INSERT INTO hops_v1 VALUES (?,?,?,?,?,?)',
                         (chain_id, number, hop.url, hop.status,
                          json.dumps(hop.headers, ensure_ascii=False),
                          None if hop.extra_headers is None else
                          json.dumps(hop.extra_headers, ensure_ascii=False)))
        receipt = {
            'receipt_id': 'receipt:' + chain_id, 'chain_id': chain_id,
            'job_id': job_id, 'attempt': attempt, 'state': state,
            'final_url': result.hops[-1].url if result.hops else None,
            'body_ref': body_ref,
            'body_sha256': None if body is None else body['sha256'],
            'failure': result.failure,
        }
        conn.execute('INSERT INTO receipts_v1 VALUES (?,?,?)',
                     (receipt['receipt_id'], chain_id,
                      json.dumps(receipt, ensure_ascii=False, sort_keys=True)))
        self.db.trip('archive.after')
        return chain_id, state

    def get_chain(self, chain_id):
        conn = self.db.connection
        row = conn.execute('SELECT * FROM chains_v1 WHERE chain_id=?',
                           (chain_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result['hops'] = []
        for hop in conn.execute(
                'SELECT * FROM hops_v1 WHERE chain_id=? ORDER BY position',
                (chain_id,)):
            result['hops'].append({
                'position': hop['position'], 'url': hop['url'],
                'status': hop['status'],
                'headers': json.loads(hop['headers_json']),
                'extra_headers': None if hop['extra_json'] is None else
                json.loads(hop['extra_json']),
            })
        return result

    def get_receipt(self, chain_id):
        row = self.db.connection.execute(
            'SELECT payload_json FROM receipts_v1 WHERE chain_id=?',
            (chain_id,)).fetchone()
        return None if row is None else json.loads(row[0])

    def read_body(self, body_ref):
        return Bodies(self.db).read_body(body_ref)
