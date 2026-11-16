from capture import capture, run
from capture.errors import InvalidState, ProtocolError
from vendor import sdk1
from tests.support import MemoryCase


class CollectionTests(MemoryCase):
    def test_single_and_empty(self):
        job, chain_id = self.collect('single')
        chain = self.archive.get_chain(chain_id)
        self.assertEqual(chain['state'], 'succeeded')
        self.assertEqual(chain['body_position'], 0)
        self.assertEqual(chain['hops'], [{
            'position': 0, 'url': 'https://records.example/item', 'status': 200,
            'headers': [['Content-Type', 'text/plain']], 'extra_headers': None,
        }])
        self.assertEqual(self.bodies.read_body(chain['body_ref']), b'hello\x00\xe4\xb8\xad')
        self.assertEqual(self.jobs.get_job(job)['attempts'], [
            {'number': 1, 'state': 'succeeded', 'chain_id': chain_id}])
        _, empty_id = self.collect('empty')
        empty = self.archive.get_chain(empty_id)
        self.assertIsNotNone(empty['body_ref'])
        self.assertEqual(self.bodies.read_body(empty['body_ref']), b'')
        self.assertEqual(empty['hops'][0]['extra_headers'], [])
        self.assertEqual(self.bodies.metadata(empty['body_ref'])['size'], 0)

    def test_redirect_evidence_and_receipt(self):
        job, chain_id = self.collect('redirect')
        chain = self.archive.get_chain(chain_id)
        self.assertEqual((chain['session_id'], chain['request_id']),
                         ('legacy-b', 'shared'))
        self.assertEqual(chain['body_position'], 1)
        self.assertEqual(chain['hops'], [
            {'position': 0, 'url': 'https://records.example/start', 'status': 302,
             'headers': [['Location', 'https://cdn.example/final'],
                         ['X-Route', 'one'], ['X-Route', 'two']],
             'extra_headers': [['Set-Cookie', 'route=a'], ['Set-Cookie', 'route=b']]},
            {'position': 1, 'url': 'https://cdn.example/final', 'status': 200,
             'headers': [['X-Checksum', 'base-final'], ['X-Checksum', 'base-again']],
             'extra_headers': [['X-Checksum', 'extra-final'], ['X-Checksum', 'extra-again']]},
        ])
        self.assertEqual(self.bodies.read_body(chain['body_ref']), b'final\x00bytes')
        receipt = self.archive.get_receipt(chain_id)
        self.assertEqual(receipt, {
            'receipt_id': 'receipt:' + chain_id, 'chain_id': chain_id,
            'job_id': job, 'attempt': 1, 'state': 'succeeded',
            'final_url': 'https://cdn.example/final', 'body_ref': chain['body_ref'],
            'body_sha256': self.bodies.metadata(chain['body_ref'])['sha256'],
            'failure': None,
        })

    def test_public_capture_replay_does_not_fetch_again(self):
        from capture.samples import source
        src = source('single', 'idempotent')
        first = capture(src, self.db)
        before = self.counts()
        self.assertEqual(capture(src, self.db), first)
        self.assertEqual(src.calls, ['https://records.example/item'])
        self.assertEqual(self.counts(), before)
        with self.assertRaises(InvalidState):
            run(1, src, self.db)

    def test_http_error_is_a_successful_capture(self):
        url = 'https://records.example/missing'
        result = sdk1.CompleteChain('s', 'r', (sdk1.Hop(url, 404, ()),), b'not found')
        src = sdk1.Source([], {url: [result]})
        job = self.jobs.enqueue(url, 'http404')
        chain_id = run(job, src, self.db)['chain_id']
        chain = self.archive.get_chain(chain_id)
        self.assertEqual(chain['state'], 'succeeded')
        self.assertEqual(chain['hops'][0]['status'], 404)
        self.assertEqual(self.bodies.read_body(chain['body_ref']), b'not found')

    def test_malformed_source_never_publishes(self):
        url = 'https://records.example/item'
        malformed = [
            sdk1.CompleteChain('s', 'r', (), b''),
            sdk1.CompleteChain('s', 'r', (sdk1.Hop(url, 302, ()),), b''),
            sdk1.CompleteChain('s', 'r', (sdk1.Hop(url, 200, (('X', 'a\nb'),)),), b''),
            sdk1.CompleteChain('s', 'r', (sdk1.Hop(url, 200, ()),), b'', 'failed'),
            sdk1.CompleteChain('s', 'r', (sdk1.Hop('https://wrong.example/', 200, ()),), b''),
            sdk1.CompleteChain('s', 'r', (sdk1.Hop(url, True, ()),), b''),
        ]
        job = self.jobs.enqueue(url, 'bad-source')
        before = self.counts()
        for result in malformed:
            with self.subTest(result=result):
                with self.assertRaises(ProtocolError):
                    run(job, sdk1.Source([], {url: [result]}), self.db)
                self.assertEqual(self.counts(), before)
                self.assertEqual(self.jobs.get_job(job)['state'], 'pending')

    def test_source_port_error_is_not_network_failure(self):
        url = 'https://records.example/item'
        job = self.jobs.enqueue(url, 'exhausted')
        before = self.counts()
        with self.assertRaises(sdk1.SourceExhausted):
            run(job, sdk1.Source([], {}), self.db)
        self.assertEqual(self.counts(), before)
