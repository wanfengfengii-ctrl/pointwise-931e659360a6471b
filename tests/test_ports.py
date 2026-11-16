import unittest
from vendor import sdk1, sdk2
from capture.samples import event_player


class PortTests(unittest.TestCase):
    def test_sdk1_complete_objects_are_independent_of_sdk2(self):
        url = 'https://port.example/start'
        final = 'https://port.example/final'
        outcome = sdk1.CompleteChain('s', 'same-request', (
            sdk1.Hop(url, 302, (('Location', final),), (('X', 'redirect'),)),
            sdk1.Hop(final, 200, (('X', 'base'), ('X', 'base2')), (('X', 'final'),)),
        ), b'owned\x00body')
        src = sdk1.Source((sdk1.Target(url, 'key'),), {url: [outcome]})
        delivered = src.fetch(url)
        self.assertEqual(delivered.hops[0].extra_headers, (('X', 'redirect'),))
        self.assertEqual(delivered.hops[1].headers, (('X', 'base'), ('X', 'base2')))
        self.assertEqual(delivered.body, b'owned\x00body')
        with self.assertRaises(sdk1.SourceExhausted):
            src.fetch(url)

    def test_sdk2_interleaving_is_delivered_verbatim(self):
        observed = []
        def accept(event):
            key = (event.session_id, event.request_id)
            if isinstance(event, sdk2.Response):
                observed.append(('response', *key, event.status, event.expects_extra))
            elif isinstance(event, sdk2.ExtraHeaders):
                observed.append(('extra', *key, event.headers))
            elif isinstance(event, sdk2.BorrowedBody):
                observed.append(('body', *key, bytes(event.data)))
            elif isinstance(event, sdk2.Finished):
                observed.append(('finished', *key))
            elif isinstance(event, sdk2.Failed):
                observed.append(('failed', *key, event.reason))
        event_player().play(accept)
        self.assertEqual(observed, [
            ('response', 'tab-a', 'r', 302, True),
            ('extra', 'tab-b', 'r', (('X-Early', 'other-session'),)),
            ('response', 'tab-a', 'r', 200, True),
            ('response', 'tab-b', 'r', 200, True),
            ('extra', 'tab-a', 'r', (('X-Redirect', 'late-302'), ('X-Redirect', 'second'))),
            ('body', 'tab-a', 'r', b'AB'),
            ('body', 'tab-b', 'r', b'XY'),
            ('finished', 'tab-a', 'r'),
            ('finished', 'tab-b', 'r'),
            ('extra', 'tab-a', 'r', (('X-Final', 'after-finish'),)),
            ('response', 'tab-c', 'no-extra', 204, False),
            ('body', 'tab-c', 'no-extra', b''),
            ('finished', 'tab-c', 'no-extra'),
            ('response', 'tab-d', 'broken', 302, True),
            ('failed', 'tab-d', 'broken', 'TLS handshake failed'),
            ('extra', 'tab-d', 'broken', (('X-Trace', 'confirmed-before-failure'),)),
        ])

    def test_body_is_borrowed_only_during_callback(self):
        player = sdk2.Player([sdk2.BodyChunk('s', 'r', b'AB'),
                              sdk2.BodyChunk('s', 'q', b'XY')])
        views, owned = [], []
        def accept(event):
            views.append(event.data)
            owned.append(bytes(event.data))
        player.play(accept)
        self.assertEqual(owned, [b'AB', b'XY'])
        self.assertEqual([bytes(v) for v in views], [b'\xa5\xa5', b'\xa5\xa5'])
        self.assertIs(views[0].obj, views[1].obj)

    def test_callback_error_propagates_and_invalidates_buffer(self):
        views = []
        def stop(event):
            views.append(event.data)
            raise RuntimeError('consumer stopped')
        with self.assertRaisesRegex(RuntimeError, 'consumer stopped'):
            sdk2.Player([sdk2.BodyChunk('s', 'r', b'abc')]).play(stop)
        self.assertEqual(bytes(views[0]), b'\xa5' * 3)

    def test_raw_port_does_not_assemble_or_wait_for_evidence(self):
        # An incomplete fragment is observable; completeness belongs to consumers.
        response = sdk2.Response('s', 'r', 'https://port.example/', 200, (), True)
        finished = sdk2.Finished('s', 'r')
        seen = []
        sdk2.Player([response, finished]).play(seen.append)
        self.assertEqual(seen, [response, finished])
        with self.assertRaises(sdk2.PortError):
            sdk2.from_records([{'kind': 'unknown', 'session_id': 's', 'request_id': 'r'}])
        with self.assertRaises(sdk2.PortError):
            sdk2.Player([sdk2.BodyChunk('', 'r', b'')])
