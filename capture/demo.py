import json
import tempfile
from pathlib import Path
from . import Archive, Bodies, Database, Jobs
from .samples import build_history


def main():
    with tempfile.TemporaryDirectory(prefix='response-capture-demo-') as directory:
        path = Path(directory) / 'history.sqlite'
        ids = build_history(path)
        with Database(path) as db:
            archive = Archive(db)
            results = []
            for chain_id in ids['chains']:
                chain = archive.get_chain(chain_id)
                body = None
                if chain['body_ref'] is not None:
                    body = Bodies(db).read_body(chain['body_ref']).hex()
                results.append({
                    'chain_id': chain_id, 'state': chain['state'],
                    'urls': [hop['url'] for hop in chain['hops']],
                    'body_hex': body, 'failure': chain['failure'],
                    'receipt': archive.get_receipt(chain_id)['receipt_id'],
                })
            print(json.dumps({
                'storage_version': db.connection.execute(
                    "SELECT value FROM meta WHERE key='storage_version'").fetchone()[0],
                'retry_states': [a['state'] for a in
                                 Jobs(db).get_job(ids['retry_job'])['attempts']],
                'chains': results,
            }, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
