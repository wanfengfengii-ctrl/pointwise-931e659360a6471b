from capture import capture, run
from capture.samples import source
from vendor import sdk1
from tests.support import MemoryCase


class RetryTests(MemoryCase):
    def test_failed_redirect_then_explicit_retry(self):
        job, failed_id = self.collect('failed', 'retry-key', raise_network=True)
        failed = self.archive.get_chain(failed_id)
        receipt = self.archive.get_receipt(failed_id)
        self.assertEqual(failed['state'], 'failed')
        self.assertEqual(failed['failure'], 'connection reset after redirect')
        self.assertIsNone(failed['body_ref'])
        self.assertIsNone(failed['body_position'])
        self.assertEqual(failed['hops'], [{
            'position': 0, 'url': 'https://records.example/start', 'status': 302,
            'headers': [['Location', 'https://cdn.example/final']],
            'extra_headers': [['X-Trace', 'redirect-confirmed']],
        }])
        self.assertEqual(receipt['state'], 'failed')
        self.assertIsNone(receipt['body_sha256'])
        # A repeated request is a replay, not an implicit retry.
        replay = source('failed', 'retry-key')
        self.assertEqual(capture(replay, self.db),
                         [{'chain_id': failed_id, 'state': 'failed'}])
        self.assertEqual(replay.calls, [])
        self.assertEqual(self.jobs.retry(job), 2)
        success_id = run(job, source('redirect'), self.db)['chain_id']
        self.assertNotEqual(success_id, failed_id)
        self.assertEqual(self.archive.get_chain(failed_id), failed)
        self.assertEqual(self.archive.get_receipt(failed_id), receipt)
        self.assertEqual([a['state'] for a in self.jobs.get_job(job)['attempts']],
                         ['failed', 'succeeded'])
        self.assertEqual(self.bodies.read_body(
            self.archive.get_chain(success_id)['body_ref']), b'final\x00bytes')

    def test_failure_without_response_retains_reason(self):
        url = 'https://records.example/unreachable'
        outcome = sdk1.NetworkFailure('s', 'r', (), 'DNS lookup failed')
        job = self.jobs.enqueue(url, 'dns')
        chain_id = run(job, sdk1.Source([], {url: [outcome]}), self.db)['chain_id']
        chain = self.archive.get_chain(chain_id)
        self.assertEqual(chain['hops'], [])
        self.assertEqual(chain['failure'], 'DNS lookup failed')
        self.assertEqual(self.archive.get_receipt(chain_id)['final_url'], None)
        self.assertEqual(self.counts(), (1, 1, 1, 0, 0, 1))

    def test_complete_failure_result_has_same_business_semantics(self):
        _, chain_id = self.collect('failed')
        self.assertEqual(self.archive.get_chain(chain_id)['state'], 'failed')
        self.assertEqual(self.counts(), (1, 1, 1, 1, 0, 1))
