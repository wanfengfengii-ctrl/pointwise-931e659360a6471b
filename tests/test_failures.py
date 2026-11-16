from capture import run
from capture.errors import InjectedFailure, InvalidState
from capture.samples import source
from tests.support import MemoryCase


class FailureTests(MemoryCase):
    def assert_rollback_then_rerun(self, stage):
        src = source('redirect', 'rollback:' + stage)
        job = self.jobs.enqueue(src.targets[0].url, src.targets[0].request_key)
        before = self.counts()
        before_job = self.jobs.get_job(job)
        self.fault.arm(stage)
        with self.assertRaisesRegex(InjectedFailure, stage):
            run(job, src, self.db)
        self.assertEqual(self.counts(), before)
        self.assertEqual(self.jobs.get_job(job), before_job)
        self.assertIsNone(self.archive.get_chain(f'c{job}.1'))
        self.assertIsNone(self.archive.get_receipt(f'c{job}.1'))
        self.assertFalse(self.db.connection.in_transaction)
        # Infrastructure rollback leaves the same pending attempt, not a
        # fictitious network failure; the caller may refetch that attempt.
        result = run(job, source('redirect'), self.db)
        self.assertEqual(result, {'chain_id': f'c{job}.1', 'state': 'succeeded'})
        self.assertEqual(len(self.jobs.get_job(job)['attempts']), 1)
        self.assertEqual(self.counts(), (1, 1, 1, 2, 1, 1))
        self.assertEqual(self.db.connection.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_before_body_storage(self):
        self.assert_rollback_then_rerun('body.before')

    def test_after_body_storage(self):
        self.assert_rollback_then_rerun('body.after')

    def test_publication_boundaries(self):
        self.assert_rollback_then_rerun('archive.after')

    def test_job_publication_boundary(self):
        self.assert_rollback_then_rerun('jobs.after')

    def test_commit_boundary(self):
        self.assert_rollback_then_rerun('before_commit')

    def test_failed_chain_publication_is_atomic_too(self):
        src = source('failed')
        job = self.jobs.enqueue(src.targets[0].url, 'failed-rollback')
        self.fault.arm('jobs.after')
        with self.assertRaises(InjectedFailure):
            run(job, src, self.db)
        self.assertEqual(self.counts(), (1, 1, 0, 0, 0, 0))
        run(job, source('failed'), self.db)
        self.assertEqual(self.counts(), (1, 1, 1, 1, 0, 1))

    def test_enqueue_and_retry_rollback(self):
        self.fault.arm('before_commit')
        with self.assertRaises(InjectedFailure):
            self.jobs.enqueue('https://records.example/x', 'x')
        self.assertEqual(self.counts(), (0, 0, 0, 0, 0, 0))
        job, failed = self.collect('failed')
        before = self.jobs.get_job(job)
        self.fault.arm('before_commit')
        with self.assertRaises(InjectedFailure):
            self.jobs.retry(job)
        self.assertEqual(self.jobs.get_job(job), before)
        self.assertEqual(self.jobs.retry(job), 2)
        self.assertEqual(self.archive.get_chain(failed)['state'], 'failed')

    def test_nested_transaction_is_rejected(self):
        with self.db.atomic():
            with self.assertRaises(InvalidState):
                with self.db.atomic():
                    self.fail('nested transaction was entered')
