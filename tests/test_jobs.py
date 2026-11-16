from capture.errors import Conflict, InvalidInput, InvalidState, NotFound
from tests.support import MemoryCase


class JobTests(MemoryCase):
    def test_request_key_binding(self):
        url = 'https://records.example/item'
        job = self.jobs.enqueue(url, 'request-1')
        self.assertEqual(self.jobs.enqueue(url, 'request-1'), job)
        before = self.counts()
        with self.assertRaises(Conflict):
            self.jobs.enqueue('https://records.example/other', 'request-1')
        self.assertEqual(self.counts(), before)
        other = self.jobs.enqueue(url, 'request-2')
        self.assertNotEqual(other, job)
        self.assertEqual(self.jobs.get_job(other)['state'], 'pending')

    def test_invalid_inputs_leave_database_unchanged(self):
        for url, key in [('file:///tmp/x', 'x'), ('https://', 'x'),
                         ('https://host:bad/path', 'x'),
                         ('https://u:p@host/path', 'x'),
                         ('https://host/a b', 'x'), ('https://host/', '')]:
            with self.subTest(url=url, key=key):
                with self.assertRaises(InvalidInput):
                    self.jobs.enqueue(url, key)
                self.assertEqual(self.counts(), (0, 0, 0, 0, 0, 0))

    def test_unknown_and_retry_guards(self):
        self.assertIsNone(self.jobs.get_job(999))
        self.assertIsNone(self.archive.get_chain('missing'))
        self.assertIsNone(self.archive.get_receipt('missing'))
        with self.assertRaises(NotFound):
            self.jobs.retry(999)
        with self.assertRaises(NotFound):
            self.jobs.pending(999)
        pending = self.jobs.enqueue('https://records.example/pending', 'pending')
        with self.assertRaises(InvalidState):
            self.jobs.retry(pending)
        complete, _ = self.collect('single')
        with self.assertRaises(InvalidState):
            self.jobs.retry(complete)
