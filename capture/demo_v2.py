"""Separated-event capture: interleaving, late extras, buffers, resumption.

Run with ``python3.11 -B -m capture.demo_v2``.  Creates a temporary v1 history
through the legacy entry points, upgrades it, then captures the fixed SDK2
event sample in bounded segments across a close/reopen.  Each comparison is
printed as ``expected`` versus the value served by the legacy query API.
"""
import json
import tempfile
from pathlib import Path

from vendor import sdk2

from . import Archive, Bodies, Database, capture_v2, upgrade
from .errors import ProtocolError
from .samples import build_history, event_player
from .v2 import stream_state


def _hop(chain, position):
    return chain['hops'][position]


def _show(label, expected, actual):
    return {'check': label, 'expected': expected, 'actual': actual,
            'match': expected == actual}


def main():
    report = []
    with tempfile.TemporaryDirectory(prefix='capture-v2-demo-') as directory:
        path = Path(directory) / 'history.sqlite'
        legacy = build_history(path)

        with Database(path) as db:
            # Legacy history must remain fully queryable before and after the
            # upgrade; the upgrade is idempotent.
            before = [Archive(db).get_chain(c) for c in legacy['chains']]
            upgrade(db)
            upgrade(db)
            after = [Archive(db).get_chain(c) for c in legacy['chains']]
            report.append(_show('v1 history unchanged by upgrade',
                                before, after))
            report.append(_show('storage_version after upgrade',
                                '2', db.connection.execute(
                                    "SELECT value FROM meta WHERE key='storage_version'"
                                ).fetchone()[0]))

        player = event_player()
        all_events = len(player.events)
        first = second = None

        # First segment: 9 events.  tab-b finishes with its early extra and
        # body and is sealed now; tab-a is finished but its late final extra
        # has not arrived, so it stays staged.
        with Database(path) as db:
            first = capture_v2(player, db, limit=9)
        report.append(_show('first segment seals only complete chains',
                            [{'chain_id': 'c6.1', 'state': 'succeeded'}],
                            list(first)))
        with Database(path) as reopened:
            # Same sequence is offered again; capture resumes at the durable
            # cursor and must not republish anything already sealed.
            stats = stream_state(reopened, first.stream_id)
            report.append(_show('resume cursor after interruption',
                                (9, False), (stats['cursor'], stats['complete'])))
            second = capture_v2(player, reopened)
            archive = Archive(reopened)
            chains = {}
            for result in list(first) + list(second):
                chains[result['chain_id']] = archive.get_chain(result['chain_id'])
            sealed_after_resume = [r['chain_id'] for r in second]
            report.append(_show('resume seals the remaining chains only',
                                ['c7.1', 'c8.1', 'c9.1'], sealed_after_resume))

            by_key = {(c['session_id'], c['request_id']): c for c in chains.values()}
            tab_a_body = Bodies(reopened).read_body(
                by_key[('tab-b', 'r')]['body_ref']).hex()
            empty_body = Bodies(reopened).read_body(
                by_key[('tab-c', 'no-extra')]['body_ref']).decode()

        # A third replay of the very same sequence seals nothing new.
        with Database(path) as db:
            again = capture_v2(player, db)
        report.append(_show('replay after full stream seals nothing',
                            [], list(again)))

        report.append(_show(
            'chained key and hop numbering',
            [{'position': 0, 'url': 'https://records.example/start', 'status': 302},
             {'position': 1, 'url': 'https://cdn.example/final', 'status': 200}],
            [{'position': _hop(by_key[('tab-a', 'r')], i)['position'],
              'url': _hop(by_key[('tab-a', 'r')], i)['url'],
              'status': _hop(by_key[('tab-a', 'r')], i)['status']}
             for i in range(len(by_key[('tab-a', 'r')]['hops']))]))
        report.append(_show(
            'late extras assigned only by expects_extra order',
            [{'position': 0, 'extra': [['X-Redirect', 'late-302'],
                                       ['X-Redirect', 'second']]},
             {'position': 1, 'extra': [['X-Final', 'after-finish']]}],
            [{'position': hop['position'], 'extra': hop['extra_headers']}
             for hop in by_key[('tab-a', 'r')]['hops']]))
        report.append(_show(
            'same request id in another session is a different chain',
            ('https://other.example/item', [['X-Early', 'other-session']], '5859'),
             (by_key[('tab-b', 'r')]['hops'][0]['url'],
             by_key[('tab-b', 'r')]['hops'][0]['extra_headers'],
             tab_a_body)))
        report.append(_show(
            'empty body is a present body, no extra required',
            ('', None),
            (empty_body,
             by_key[('tab-c', 'no-extra')]['hops'][0]['extra_headers'])))
        broken = by_key[('tab-d', 'broken')]
        report.append(_show(
            'failure keeps confirmed hop and late extra, no body',
            ('TLS handshake failed',
             [{'position': 0, 'status': 302,
               'extra': [['X-Trace', 'confirmed-before-failure']]}],
             None),
            (broken['failure'],
             [{'position': hop['position'], 'status': hop['status'],
               'extra': hop['extra_headers']} for hop in broken['hops']],
             broken['body_ref'])))

        # Body buffering: a trace with two body chunks on one chain and one on
        # another.  The port reuses its buffer, so only in-callback copies are
        # safe; the archive must show the original bytes.
        reuse = sdk2.Player([
            sdk2.Response('s1', 'r', 'https://reuse.example/', 200, (), False),
            sdk2.BodyChunk('s1', 'r', b'AAAA'),
            sdk2.BodyChunk('s1', 'r', b'BB'),
            sdk2.Finished('s1', 'r'),
            sdk2.Response('s2', 'r', 'https://reuse.example/', 200, (), False),
            sdk2.BodyChunk('s2', 'r', b'CCCC'),
            sdk2.Finished('s2', 'r'),
        ])
        with Database(path) as db:
            captured = capture_v2(reuse, db)
            bodies = [Bodies(db).read_body(
                Archive(db).get_chain(r['chain_id'])['body_ref'])
                for r in captured]
        report.append(_show('borrowed body chunks are copied',
                            ['414141414242', '43434343'],
                            [b.hex() for b in bodies]))

        # A malformed sequence leaves earlier durable chains intact and never
        # publishes a half chain.
        bad = sdk2.Player([
            sdk2.Response('s3', 'r', 'https://reuse.example/', 200, (), False),
            sdk2.BodyChunk('s3', 'r', b'ok'),
            sdk2.Finished('s3', 'r'),
            sdk2.Response('s3', 'r', 'https://reuse.example/again', 302, (), True),
        ])
        try:
            with Database(path) as db:
                capture_v2(bad, db)
            bad_error = None
        except ProtocolError as exc:
            bad_error = str(exc)
        report.append(_show('protocol error after a durable chain',
                            'response after chain was sealed', bad_error))
        with Database(path) as db:
            stages = db.connection.execute(
                'SELECT count(*) FROM v2_staged_chains WHERE sealed=0').fetchone()[0]
        report.append(_show('no half-published staging rows at archive level',
                            0, stages))

        report.append({'v1_history_chains': legacy['chains'],
                       'event_count': all_events,
                       'first_segment': list(first),
                       'resumed': list(second)})

    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
