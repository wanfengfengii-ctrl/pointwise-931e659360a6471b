"""Independent checks for SDK2 capture, resumption, upgrade and mixed history."""
import tempfile
import unittest
from pathlib import Path

from vendor import sdk1, sdk2

from capture import (
    Archive, Bodies, Database, FaultPlan, Jobs, capture_v2, run,
    stream_state, upgrade,
)
from capture.errors import (
    InjectedFailure, InvalidInput, ProtocolError,
)
from capture.samples import build_history, event_player, source


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


def keyed(db, results):
    archive = Archive(db)
    return {(c['session_id'], c['request_id']): c
            for c in (archive.get_chain(r['chain_id']) for r in results)}


class ReuseTrace(unittest.TestCase):
    """Two sessions with the same request_id and reused body buffers."""

    def player(self):
        return sdk2.Player([
            sdk2.Response('s1', 'r', 'https://reuse.example/', 200, (), False),
            sdk2.BodyChunk('s1', 'r', b'AAAA'),
            sdk2.BodyChunk('s1', 'r', b'BB'),
            sdk2.Finished('s1', 'r'),
            sdk2.Response('s2', 'r', 'https://reuse.example/', 200, (), False),
            sdk2.BodyChunk('s2', 'r', b'CCCC'),
            sdk2.Finished('s2', 'r'),
        ])

    def test_borrowed_chunks_are_copied_per_chain(self):
        with Database() as db:
            player = self.player()
            captured = capture_v2(player, db)
            self.assertEqual([r['state'] for r in captured],
                             ['succeeded', 'succeeded'])
            first, second = keyed(db, captured).values()
            self.assertEqual(Bodies(db).read_body(first['body_ref']), b'AAAABB')
            self.assertEqual(Bodies(db).read_body(second['body_ref']), b'CCCC')
            self.assertNotEqual(first['chain_id'], second['chain_id'])
            self.assertEqual((first['session_id'], first['request_id']),
                             ('s1', 'r'))
            self.assertEqual((second['session_id'], second['request_id']),
                             ('s2', 'r'))


class InterleaveTests(unittest.TestCase):
    def test_interleaved_sessions_and_late_extras(self):
        with Database() as db:
            captured = capture_v2(event_player(), db)
            self.assertEqual(len(captured), 4)
            self.assertTrue(captured.complete)
            self.assertEqual(captured.cursor, captured.event_count)
            tasks = keyed(db, captured)

            a = tasks[('tab-a', 'r')]
            self.assertEqual(a['state'], 'succeeded')
            self.assertEqual(a['body_position'], 1)
            self.assertEqual(
                [(h['url'], h['status'], h['headers'], h['extra_headers'])
                 for h in a['hops']], [
                    ('https://records.example/start', 302,
                     [['Location', 'https://cdn.example/final'],
                      ['X-Base', 'a'], ['X-Base', 'b']],
                     [['X-Redirect', 'late-302'], ['X-Redirect', 'second']]),
                    ('https://cdn.example/final', 200,
                     [['X-Base', 'final']],
                     [['X-Final', 'after-finish']]),
                ])
            self.assertEqual(Bodies(db).read_body(a['body_ref']), b'AB')
            self.assertEqual(Archive(db).get_receipt(a['chain_id'])['final_url'],
                             'https://cdn.example/final')

            b = tasks[('tab-b', 'r')]
            self.assertEqual(b['hops'][0]['url'], 'https://other.example/item')
            self.assertEqual(b['hops'][0]['extra_headers'],
                             [['X-Early', 'other-session']])
            self.assertEqual(Bodies(db).read_body(b['body_ref']), b'XY')
            self.assertNotEqual(a['chain_id'], b['chain_id'])
            self.assertNotEqual(a['body_ref'], b['body_ref'])

            c = tasks[('tab-c', 'no-extra')]
            self.assertEqual(c['hops'][0]['status'], 204)
            self.assertIsNone(c['hops'][0]['extra_headers'])
            self.assertEqual(Bodies(db).read_body(c['body_ref']), b'')

            d = tasks[('tab-d', 'broken')]
            self.assertEqual(d['state'], 'failed')
            self.assertEqual(d['failure'], 'TLS handshake failed')
            self.assertIsNone(d['body_ref'])
            self.assertIsNone(d['body_position'])
            self.assertEqual(d['hops'], [{
                'position': 0, 'url': 'https://broken.example/item', 'status': 302,
                'headers': [['Location', 'https://broken.example/next']],
                'extra_headers': [['X-Trace', 'confirmed-before-failure']],
            }])
            receipt = Archive(db).get_receipt(d['chain_id'])
            self.assertEqual(receipt['state'], 'failed')
            self.assertIsNone(receipt['body_sha256'])
            self.assertEqual(receipt['final_url'], 'https://broken.example/item')
            self.assertEqual(
                db.connection.execute('PRAGMA foreign_key_check').fetchall(), [])
            jobs = Jobs(db)
            for chain in (a, b, c, d):
                job = jobs.get_job(chain['job_id'])
                self.assertEqual(job['state'], chain['state'])
                self.assertEqual(job['attempts'],
                                 [{'number': 1, 'state': chain['state'],
                                   'chain_id': chain['chain_id']}])

    def test_extra_headers_are_assigned_by_expects_extra_not_hop_order(self):
        player = sdk2.Player([
            sdk2.Response('s', 'r', 'https://e.example/a', 301, (), False),
            sdk2.Response('s', 'r', 'https://e.example/b', 302, (), True),
            sdk2.Response('s', 'r', 'https://e.example/c', 302, (), True),
            sdk2.Response('s', 'r', 'https://e.example/d', 200, (), False),
            sdk2.ExtraHeaders('s', 'r', (('X-For', 'b'),)),
            sdk2.BodyChunk('s', 'r', b'done'),
            sdk2.Finished('s', 'r'),
            sdk2.ExtraHeaders('s', 'r', (('X-For', 'c'),)),
        ])
        with Database() as db:
            chain = keyed(db, capture_v2(player, db))[('s', 'r')]
        self.assertEqual([h['extra_headers'] for h in chain['hops']],
                         [None, [['X-For', 'b']], [['X-For', 'c']], None])

    def test_network_end_waits_for_declared_extras(self):
        player = sdk2.Player([
            sdk2.Response('s', 'r', 'https://wait.example/', 200, (), True),
            sdk2.BodyChunk('s', 'r', b'body'),
            sdk2.Finished('s', 'r'),
        ])
        with Database() as db:
            self.assertEqual(capture_v2(player, db), [])
            self.assertIsNone(Archive(db).get_chain('c1.1'))
            self.assertEqual(db.connection.execute(
                'SELECT count(*) FROM jobs').fetchone()[0], 0)
            # Replaying the same prefix again still publishes nothing.
            self.assertEqual(capture_v2(player, db), [])

        complete = sdk2.Player([
            sdk2.Response('s', 'r', 'https://wait.example/', 200, (), True),
            sdk2.BodyChunk('s', 'r', b'body'),
            sdk2.Finished('s', 'r'),
            sdk2.ExtraHeaders('s', 'r', ()),
        ])
        with Database() as db:
            captured = capture_v2(complete, db)
            self.assertEqual(len(captured), 1)
            chain = Archive(db).get_chain(captured[0]['chain_id'])
            self.assertEqual(chain['hops'][0]['extra_headers'], [])

    def test_success_without_body_is_not_sealed(self):
        player = sdk2.Player([
            sdk2.Response('s', 'r', 'https://nobody.example/', 200, (), False),
            sdk2.Finished('s', 'r'),
        ])
        with Database() as db:
            self.assertEqual(capture_v2(player, db), [])
            self.assertTrue(stream_state(db, capture_v2(player, db).stream_id)
                            ['complete'])
            self.assertIsNone(Archive(db).get_chain('c1.1'))


class ResumeTests(unittest.TestCase):
    def test_limit_and_replay_never_republish(self):
        with tempfile.TemporaryDirectory(prefix='capture-v2-resume-') as directory:
            path = Path(directory) / 'db.sqlite'
            player = event_player()
            with Database(path) as db:
                first = capture_v2(player, db, limit=1)
                self.assertEqual(list(first), [])
                self.assertEqual(first.cursor, 1)
                self.assertFalse(first.complete)
            with Database(path) as db:
                second = capture_v2(player, db, limit=8)
                self.assertEqual(second.cursor, 9)
                self.assertEqual([r['chain_id'] for r in second], ['c1.1'])
            with Database(path) as db:
                before = snapshot(db)
                third = capture_v2(player, db)
                self.assertEqual(third.cursor, len(player.events))
                self.assertTrue(third.complete)
                total = len(list(third))
                self.assertEqual(total, 3)
                # Sealed chains from earlier calls are served by legacy reads.
                jobs = Jobs(db).get_job(1)
                self.assertEqual(jobs['state'], 'succeeded')
                self.assertEqual(Jobs(db).get_job(jobs['job_id'])['attempts'][0]
                                 ['chain_id'], 'c1.1')
                settled = snapshot(db)
            with Database(path) as db:
                again = capture_v2(player, db)
                self.assertEqual(list(again), [])
                self.assertEqual(again.cursor, len(player.events))
                self.assertEqual(snapshot(db), settled)
                self.assertEqual(Archive(db).get_chain('c1.1')['state'],
                                 'succeeded')
                # A full replay of the same stream must not change prior rows.
                self.assertNotEqual(settled, before)

    def test_zero_limit_and_none_semantics(self):
        with Database() as db:
            self.assertEqual(capture_v2(event_player(), db, limit=0), [])
            self.assertEqual(capture_v2(event_player(), db, limit=0).cursor, 0)
        with Database() as db:
            captured = capture_v2(event_player(), db)
            self.assertEqual(len(captured), 4)
        with self.assertRaises(InvalidInput):
            with Database() as db:
                capture_v2(event_player(), db, limit=-1)
        with self.assertRaises(InvalidInput):
            with Database() as db:
                capture_v2(event_player(), db, limit=True)

    def test_interruption_between_events_keeps_prior_seals(self):
        player = sdk2.Player([
            sdk2.Response('a', 'x', 'https://i.example/x', 200, (), False),
            sdk2.BodyChunk('a', 'x', b'one'),
            sdk2.Finished('a', 'x'),
            sdk2.Response('b', 'y', 'https://i.example/y', 200, (), False),
            sdk2.BodyChunk('b', 'y', b'two'),
            sdk2.Finished('b', 'y'),
        ])
        fault = FaultPlan()
        with tempfile.TemporaryDirectory(prefix='capture-v2-fault-') as directory:
            path = Path(directory) / 'db.sqlite'
            with Database(path, fault=fault) as db:
                fault.arm('v2.event.before', after=4)
                with self.assertRaises(InjectedFailure):
                    capture_v2(player, db)
                self.assertEqual(
                    [r[0] for r in db.connection.execute(
                        'SELECT chain_id FROM chains_v1 ORDER BY chain_id')],
                    ['c1.1'])
                sid = db.connection.execute(
                    'SELECT stream_id FROM v2_streams').fetchone()[0]
                self.assertEqual(stream_state(db, sid)['cursor'], 4)
            with Database(path) as db:
                resumed = capture_v2(player, db)
                self.assertEqual([r['chain_id'] for r in resumed], ['c2.1'])
                self.assertEqual(
                    [r[0] for r in db.connection.execute(
                        'SELECT chain_id FROM chains_v1 ORDER BY chain_id')],
                    ['c1.1', 'c2.1'])

    def test_interruption_at_seal_boundaries_is_atomic(self):
        for stage in ('v2.seal.before', 'body.after', 'archive.after',
                      'jobs.after'):
            with self.subTest(stage=stage):
                with Database(fault=FaultPlan()) as db:
                    db.fault.arm(stage)
                    with self.assertRaises(InjectedFailure):
                        capture_v2(sdk2.Player([
                            sdk2.Response('s', 'r', 'https://atomic.example/',
                                          200, (), False),
                            sdk2.BodyChunk('s', 'r', b'body'),
                            sdk2.Finished('s', 'r'),
                        ]), db)
                    self.assertEqual(db.connection.execute(
                        'SELECT count(*) FROM chains_v1').fetchone()[0], 0)
                    self.assertEqual(db.connection.execute(
                        'SELECT count(*) FROM jobs').fetchone()[0], 0)
                    # The failing event rolled back; earlier events stay.
                    self.assertEqual(db.connection.execute(
                        'SELECT cursor FROM v2_streams').fetchone()[0], 2)
                    self.assertEqual(db.connection.execute(
                        'SELECT count(*) FROM v2_staged_chains '
                        'WHERE sealed=1').fetchone()[0], 0)
                    self.assertFalse(db.connection.in_transaction)
                    # Replaying finishes the same attempt without duplicates.
                    resumed = capture_v2(sdk2.Player([
                        sdk2.Response('s', 'r', 'https://atomic.example/',
                                      200, (), False),
                        sdk2.BodyChunk('s', 'r', b'body'),
                        sdk2.Finished('s', 'r'),
                    ]), db)
                    self.assertEqual(len(resumed), 1)
                    self.assertEqual(db.connection.execute(
                        'SELECT count(*) FROM jobs').fetchone()[0], 1)

    def test_commit_boundary_fault_keeps_database_at_version_1(self):
        with Database(fault=FaultPlan()) as db:
            db.fault.arm('before_commit')  # fires inside the implicit upgrade
            with self.assertRaises(InjectedFailure):
                capture_v2(sdk2.Player([
                    sdk2.Response('s', 'r', 'https://atomic.example/', 200,
                                  (), False),
                    sdk2.BodyChunk('s', 'r', b'body'),
                    sdk2.Finished('s', 'r'),
                ]), db)
            self.assertEqual(db.connection.execute(
                "SELECT value FROM meta WHERE key='storage_version'"
            ).fetchone()[0], '1')
            self.assertEqual(db.connection.execute(
                "SELECT count(*) FROM sqlite_master WHERE name='v2_streams'"
            ).fetchone()[0], 0)
            self.assertEqual(db.connection.execute(
                'SELECT count(*) FROM jobs').fetchone()[0], 0)
            self.assertFalse(db.connection.in_transaction)

    def test_changed_sequence_under_same_identity_is_rejected(self):
        player = event_player()
        with Database() as db:
            capture_v2(player, db, limit=1)
            other = sdk2.Player(list(player.events)[:-1])
            from capture.v2 import stream_id
            # Different event list has a different identity, so it is a new
            # stream rather than a mutation of the interrupted one.
            self.assertNotEqual(stream_id(list(player.events)),
                                stream_id(list(other.events)))
            capture_v2(other, db, limit=0)


class ProtocolTests(unittest.TestCase):
    def assert_rejected_without_publishing(self, events):
        with Database(fault=FaultPlan()) as db:
            with self.assertRaises(ProtocolError):
                capture_v2(sdk2.Player(events), db)
            self.assertEqual(db.connection.execute(
                'SELECT count(*) FROM chains_v1').fetchone()[0], 0)
            self.assertEqual(db.connection.execute(
                'SELECT count(*) FROM jobs').fetchone()[0], 0)

    def test_duplicate_network_end(self):
        self.assert_rejected_without_publishing([
            sdk2.Response('s', 'r', 'https://p.example/', 200, (), False),
            sdk2.Finished('s', 'r'),
            sdk2.Failed('s', 'r', 'boom'),
        ])

    def test_response_after_network_end(self):
        self.assert_rejected_without_publishing([
            sdk2.Response('s', 'r', 'https://p.example/', 302, (), False),
            sdk2.Finished('s', 'r'),
            sdk2.Response('s', 'r', 'https://p.example/next', 200, (), False),
        ])

    def test_extra_headers_without_declared_slot(self):
        with Database() as db:
            with self.assertRaises(ProtocolError):
                capture_v2(sdk2.Player([
                    sdk2.Response('s', 'r', 'https://p.example/', 200, (), False),
                    sdk2.ExtraHeaders('s', 'r', ()),
                    sdk2.BodyChunk('s', 'r', b''),
                    sdk2.Finished('s', 'r'),
                ]), db)
            self.assertEqual(db.connection.execute(
                'SELECT count(*) FROM chains_v1').fetchone()[0], 0)

    def test_nonfinal_hop_must_be_redirect(self):
        with Database() as db:
            with self.assertRaises(ProtocolError):
                capture_v2(sdk2.Player([
                    sdk2.Response('s', 'r', 'https://p.example/', 200, (), False),
                    sdk2.Response('s', 'r', 'https://p.example/next', 200, (), False),
                    sdk2.BodyChunk('s', 'r', b''),
                    sdk2.Finished('s', 'r'),
                ]), db)

    def test_invalid_headers_are_rejected(self):
        with Database() as db:
            with self.assertRaises(ProtocolError):
                capture_v2(sdk2.Player([
                    sdk2.Response('s', 'r', 'https://p.example/', 200,
                                  (('X', 'a\nb'),), False),
                    sdk2.BodyChunk('s', 'r', b''),
                    sdk2.Finished('s', 'r'),
                ]), db)

    def test_unknown_event_type_rejected(self):
        from capture.v2 import stream_id

        class Odd:
            pass
        with self.assertRaises(ProtocolError):
            stream_id([Odd()])
        with Database() as db:
            with self.assertRaises(InvalidInput):
                capture_v2(sdk2.Player([
                    sdk2.Response('s', 'r', 'https://p.example/', 200, (), False),
                    sdk2.BodyChunk('s', 'r', b''),
                    sdk2.Finished('s', 'r'),
                ]), db, limit='1')


class UpgradeTests(unittest.TestCase):
    def test_upgrade_preserves_v1_history_and_is_idempotent(self):
        with tempfile.TemporaryDirectory(prefix='capture-v2-upgrade-') as directory:
            path = Path(directory) / 'history.sqlite'
            ids = build_history(path)
            with Database(path) as db:
                before = snapshot(db)
                self.assertEqual(upgrade(db), 2)
                self.assertEqual(upgrade(db), 2)
                self.assertEqual(snapshot(db), before)
                self.assertEqual(db.connection.execute(
                    "SELECT value FROM meta WHERE key='storage_version'"
                ).fetchone()[0], '2')
                # Old chains stay queryable through every legacy entry point.
                chain = Archive(db).get_chain(ids['chains'][2])
                self.assertEqual(chain['hops'][0]['status'], 302)
                self.assertEqual(Bodies(db).read_body(chain['body_ref']),
                                 b'final\x00bytes')
                self.assertEqual(Archive(db).get_receipt(
                    ids['chains'][2])['state'], 'succeeded')
                self.assertEqual([a['state'] for a in Jobs(db).get_job(
                    ids['retry_job'])['attempts']], ['failed', 'succeeded'])
            with Database(path) as db:
                self.assertEqual(snapshot(db), before)
                self.assertEqual(upgrade(db), 2)

    def test_new_v2_chains_coexist_with_v1_history(self):
        with tempfile.TemporaryDirectory(prefix='capture-v2-mixed-') as directory:
            path = Path(directory) / 'history.sqlite'
            ids = build_history(path)
            with Database(path) as db:
                before = snapshot(db)
                captured = capture_v2(event_player(), db)
                after = snapshot(db)
                self.assertEqual(after[0][:len(before[0])], before[0])
                self.assertEqual(len(after[0]), len(before[0]) + len(captured))
                # SDK1 still works after the upgrade on the same database.
                extra = source('single', 'after-v2')
                job = Jobs(db).enqueue(extra.targets[0].url,
                                       extra.targets[0].request_key)
                legacy = run(job, extra, db)
                chain = Archive(db).get_chain(legacy['chain_id'])
                self.assertEqual(chain['state'], 'succeeded')
                self.assertEqual(Bodies(db).read_body(chain['body_ref']),
                                 b'hello\x00\xe4\xb8\xad')
            with Database(path) as db:
                self.assertEqual(Archive(db).get_chain(
                    captured[0]['chain_id'])['state'], 'succeeded')

    def test_interrupted_upgrade_leaves_version_1(self):
        for stage in ('migration.after', 'before_commit'):
            with self.subTest(stage=stage):
                with tempfile.TemporaryDirectory(prefix='capture-v2-half-') as directory:
                    path = Path(directory) / 'history.sqlite'
                    build_history(path)
                    fault = FaultPlan()
                    with Database(path, fault=fault) as db:
                        before = snapshot(db)
                        tables = {r[0] for r in db.connection.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'")}
                        fault.arm(stage)
                        with self.assertRaises(InjectedFailure):
                            upgrade(db)
                        self.assertEqual(db.connection.execute(
                            "SELECT value FROM meta WHERE key='storage_version'"
                        ).fetchone()[0], '1')
                        self.assertEqual({r[0] for r in db.connection.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'")},
                            tables)
                        self.assertEqual(snapshot(db), before)
                    with Database(path) as db:
                        self.assertEqual(upgrade(db), 2)
                        self.assertEqual(capture_v2(event_player(), db).cursor,
                                         len(event_player().events))


class LegacyCompatibilityTests(unittest.TestCase):
    def test_v2_capture_keeps_sdk1_regression_semantics(self):
        with Database() as db:
            job = Jobs(db).enqueue('https://records.example/item', 'sdk1-still')
            chain_id = run(job, source('single'), db)['chain_id']
            captured = capture_v2(event_player(), db)
            self.assertEqual(Archive(db).get_chain(chain_id)['state'], 'succeeded')
            self.assertEqual(Jobs(db).get_job(job)['attempts'][0]['chain_id'],
                             chain_id)
            self.assertEqual(len(captured), 4)
            self.assertEqual(db.connection.execute(
                'SELECT count(*) FROM receipts_v1').fetchone()[0], 5)

    def test_retry_after_v2_failure(self):
        player = sdk2.Player([
            sdk2.Response('s', 'r', 'https://retry.example/', 302, (), True),
            sdk2.Failed('s', 'r', 'network lost'),
            sdk2.ExtraHeaders('s', 'r', (('X-Trace', 'confirmed'),)),
        ])
        with Database() as db:
            failed = capture_v2(player, db)[0]
            job = Archive(db).get_chain(failed['chain_id'])['job_id']
            self.assertEqual(Jobs(db).retry(job), 2)
            outcome = sdk1.CompleteChain('s', 'r', (
                sdk1.Hop('https://retry.example/', 200, ()),), b'retried')
            chain_id = run(job, sdk1.Source([], {'https://retry.example/': [outcome]}),
                           db)['chain_id']
            self.assertEqual([a['state'] for a in
                              Jobs(db).get_job(job)['attempts']],
                             ['failed', 'succeeded'])
            failed_chain = Archive(db).get_chain(failed['chain_id'])
            self.assertEqual(failed_chain['state'], 'failed')
            self.assertEqual(failed_chain['hops'][0]['extra_headers'],
                             [['X-Trace', 'confirmed']])
            self.assertEqual(Bodies(db).read_body(
                Archive(db).get_chain(chain_id)['body_ref']), b'retried')


if __name__ == '__main__':
    unittest.main()
