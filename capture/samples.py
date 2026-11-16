"""Finite business examples and a real, reopenable SDK1 history builder."""
import json
from pkgutil import get_data
from vendor import sdk1, sdk2
from .db import Database
from .jobs import Jobs
from .acquisition import run


def case(name):
    record = json.loads(get_data('capture', 'sample_data.json'))[name]
    hops = tuple(sdk1.Hop(
        hop['url'], hop['status'], tuple(tuple(p) for p in hop['headers']),
        None if hop['extra_headers'] is None else
        tuple(tuple(p) for p in hop['extra_headers'])) for hop in record['hops'])
    body = None if record['body_hex'] is None else bytes.fromhex(record['body_hex'])
    result = sdk1.CompleteChain(record['session_id'], record['request_id'],
                               hops, body, record['failure'])
    return record['url'], result


def source(name, request_key=None, raise_network=False):
    url, result = case(name)
    if raise_network:
        if result.failure is None:
            raise ValueError('only a failure case can raise NetworkFailure')
        result = sdk1.NetworkFailure(result.session_id, result.request_id,
                                     result.hops, result.failure)
    return sdk1.Source([sdk1.Target(url, request_key or name)], {url: [result]})


def event_player():
    return sdk2.from_records(json.loads(get_data('capture', 'sample_events.json')))


def build_history(path):
    """Populate an empty v1 database, close it, and return stable local IDs.

    Refuses a nonempty jobs table. Includes a redirect, an empty body, a
    failed attempt followed by success, and a distinct job for the same URL.
    """
    with Database(path) as db:
        if db.connection.execute('SELECT 1 FROM jobs LIMIT 1').fetchone():
            raise ValueError('history builder requires an empty capture database')
        jobs = Jobs(db)
        chains = []
        for name in ('single', 'empty', 'redirect'):
            src = source(name, 'history:' + name)
            job = jobs.enqueue(src.targets[0].url, src.targets[0].request_key)
            chains.append(run(job, src, db)['chain_id'])
        failed_src = source('failed', 'history:retry', raise_network=True)
        retry_job = jobs.enqueue(failed_src.targets[0].url, 'history:retry')
        chains.append(run(retry_job, failed_src, db)['chain_id'])
        jobs.retry(retry_job)
        chains.append(run(retry_job, source('redirect'), db)['chain_id'])
        duplicate = source('redirect', 'history:duplicate-url')
        duplicate_job = jobs.enqueue(duplicate.targets[0].url, 'history:duplicate-url')
        chains.append(run(duplicate_job, duplicate, db)['chain_id'])
        return {'chains': chains, 'retry_job': retry_job, 'duplicate_job': duplicate_job}
