import tempfile
import unittest
from pathlib import Path
from capture import Archive, Bodies, Database, FaultPlan, Jobs, run
from capture.errors import InjectedFailure
from capture.samples import build_history, source


def snapshot(db):
    archive = Archive(db)
    chain_ids = [r[0] for r in db.connection.execute(
        'SELECT chain_id FROM chains_v1 ORDER BY chain_id')]
    chains = [archive.get_chain(c) for c in chain_ids]
    receipts = [archive.get_receipt(c) for c in chain_ids]
    bodies = {c['body_ref']: Bodies(db).read_body(c['body_ref'])
              for c in chains if c['body_ref'] is not None}
    jobs = [Jobs(db).get_job(r[0]) for r in db.connection.execute(
        'SELECT job_id FROM jobs ORDER BY job_id')]
    return chains, receipts, bodies, jobs


class HistoryTests(unittest.TestCase):
    def test_history_builder_and_reopen_preserve_all_results(self):
        with tempfile.TemporaryDirectory(prefix='capture-history-test-') as directory:
            path = Path(directory) / 'history.sqlite'
            ids = build_history(path)
            with Database(path) as db:
                before = snapshot(db)
                self.assertEqual(len(before[0]), 6)
                self.assertEqual(len(before[1]), 6)
                self.assertEqual(len(before[2]), 5)
                self.assertEqual(len(before[3]), 5)
                self.assertEqual([a['state'] for a in
                    Jobs(db).get_job(ids['retry_job'])['attempts']],
                    ['failed', 'succeeded'])
                self.assertEqual(db.connection.execute('PRAGMA foreign_key_check').fetchall(), [])
            with Database(path) as reopened:
                self.assertEqual(snapshot(reopened), before)
                jobs = Jobs(reopened)
                for job in before[3]:
                    self.assertEqual(jobs.enqueue(job['url'], job['request_key']), job['job_id'])
                extra = source('single', 'after-reopen')
                job_id = jobs.enqueue(extra.targets[0].url, 'after-reopen')
                chain_id = run(job_id, extra, reopened)['chain_id']
                self.assertNotIn(chain_id, ids['chains'])
            with Database(path) as reopened:
                self.assertEqual(Archive(reopened).get_chain(chain_id)['state'], 'succeeded')
            with self.assertRaises(ValueError):
                build_history(path)

    def test_failed_commit_close_reopen_and_rerun(self):
        with tempfile.TemporaryDirectory(prefix='capture-rollback-test-') as directory:
            path = Path(directory) / 'capture.sqlite'
            fault = FaultPlan()
            with Database(path, fault=fault) as db:
                job = Jobs(db).enqueue('https://records.example/start', 'durable')
                fault.arm('before_commit')
                with self.assertRaises(InjectedFailure):
                    run(job, source('redirect'), db)
                self.assertIsNone(Archive(db).get_chain(f'c{job}.1'))
            with Database(path) as db:
                self.assertEqual(Jobs(db).get_job(job)['state'], 'pending')
                self.assertEqual(db.connection.execute(
                    'SELECT count(*) FROM bodies_v1').fetchone()[0], 0)
                result = run(job, source('redirect'), db)
                persisted = snapshot(db)
            with Database(path) as db:
                self.assertEqual(snapshot(db), persisted)
                self.assertEqual(result['chain_id'], f'c{job}.1')
                self.assertEqual(Jobs(db).get_job(job)['attempts'],
                    [{'number': 1, 'state': 'succeeded', 'chain_id': f'c{job}.1'}])
