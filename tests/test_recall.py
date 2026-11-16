from dataclasses import replace
from capture import run
from capture.errors import IntegrityError
from capture.samples import case
from vendor import sdk1
from tests.support import MemoryCase


class RecallTests(MemoryCase):
    def test_same_url_and_sdk_key_are_distinct_captures(self):
        url, first = case('redirect')
        second = replace(first, body=b'another edition')
        src = sdk1.Source([], {url: [first, second]})
        one = self.jobs.enqueue(url, 'edition-1')
        two = self.jobs.enqueue(url, 'edition-2')
        c1 = run(one, src, self.db)['chain_id']
        c2 = run(two, src, self.db)['chain_id']
        self.assertNotEqual(c1, c2)
        a = self.archive.get_chain(c1)
        b = self.archive.get_chain(c2)
        self.assertNotEqual(a['body_ref'], b['body_ref'])
        self.assertEqual((a['session_id'], a['request_id']),
                         (b['session_id'], b['request_id']))
        self.assertEqual(self.bodies.read_body(a['body_ref']), b'final\x00bytes')
        self.assertEqual(self.bodies.read_body(b['body_ref']), b'another edition')
        self.assertEqual(self.archive.get_receipt(c1)['job_id'], one)
        self.assertEqual(self.archive.get_receipt(c2)['job_id'], two)

    def test_query_results_are_detached(self):
        job, chain_id = self.collect('redirect')
        original = self.archive.get_chain(chain_id)
        original['hops'][0]['headers'].append(['X', 'caller mutation'])
        original['hops'].clear()
        receipt = self.archive.get_receipt(chain_id)
        receipt['state'] = 'caller mutation'
        attempt = self.jobs.get_job(job)
        attempt['attempts'][0]['chain_id'] = 'caller mutation'
        self.assertEqual(len(self.archive.get_chain(chain_id)['hops']), 2)
        self.assertEqual(self.archive.get_receipt(chain_id)['state'], 'succeeded')
        self.assertEqual(self.jobs.get_job(job)['attempts'][0]['chain_id'], chain_id)

    def test_corruption_is_explicit_and_evidence_survives(self):
        _, chain_id = self.collect('redirect')
        before = self.archive.get_chain(chain_id)
        receipt = self.archive.get_receipt(chain_id)
        ref = before['body_ref']
        # Equal-length corruption exercises SHA-256, not merely byte count.
        with self.db.atomic() as conn:
            conn.execute('UPDATE bodies_v1 SET payload=? WHERE body_ref=?',
                         (b'X' * len(b'final\x00bytes'), ref))
        with self.assertRaises(IntegrityError):
            self.bodies.read_body(ref)
        self.assertEqual(self.archive.get_chain(chain_id), before)
        self.assertEqual(self.archive.get_receipt(chain_id), receipt)

    def test_length_corruption_and_missing_reference(self):
        _, chain_id = self.collect('empty')
        ref = self.archive.get_chain(chain_id)['body_ref']
        with self.db.atomic() as conn:
            conn.execute('UPDATE bodies_v1 SET size=1 WHERE body_ref=?', (ref,))
        with self.assertRaises(IntegrityError):
            self.bodies.read_body(ref)
        with self.assertRaises(IntegrityError):
            self.bodies.read_body('unknown-body')
        with self.assertRaises(IntegrityError):
            self.bodies.metadata('unknown-body')
