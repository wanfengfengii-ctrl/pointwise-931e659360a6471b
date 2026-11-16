from vendor import sdk1
from .archive import Archive
from .bodies import Bodies
from .errors import InvalidInput, InvalidState, ProtocolError
from .jobs import Jobs, validate_url

REDIRECTS = {301, 302, 303, 307, 308}


def _headers(value):
    if not isinstance(value, (list, tuple)):
        raise ProtocolError('headers must be an ordered sequence')
    result = []
    for pair in value:
        if (not isinstance(pair, (list, tuple)) or len(pair) != 2 or
                not all(isinstance(x, str) for x in pair)):
            raise ProtocolError('each header must be a text pair')
        name, text = pair
        if (not name or any(c in name for c in '\r\n:') or
                any(c in text for c in '\r\n')):
            raise ProtocolError('invalid header')
        result.append((name, text))
    return tuple(result)


def _complete(raw, target_url):
    if not isinstance(raw, sdk1.CompleteChain):
        raise ProtocolError('SDK1 must return CompleteChain')
    if (not isinstance(raw.session_id, str) or not raw.session_id or
            not isinstance(raw.request_id, str) or not raw.request_id):
        raise ProtocolError('empty chain provenance')
    if not isinstance(raw.hops, (tuple, list)):
        raise ProtocolError('hops must be an ordered sequence')
    hops = []
    for hop in raw.hops:
        if not isinstance(hop, sdk1.Hop):
            raise ProtocolError('invalid hop')
        try:
            validate_url(hop.url)
        except InvalidInput as exc:
            raise ProtocolError('invalid response URL') from exc
        if type(hop.status) is not int or not 200 <= hop.status <= 599:
            raise ProtocolError('invalid final response status')
        hops.append(sdk1.Hop(hop.url, hop.status, _headers(hop.headers),
                    None if hop.extra_headers is None else _headers(hop.extra_headers)))
    if hops and hops[0].url != target_url:
        raise ProtocolError('first hop does not match requested URL')
    if any(hop.status not in REDIRECTS for hop in hops[:-1]):
        raise ProtocolError('nonfinal hop must be a redirect')
    if raw.failure is None:
        if not hops or hops[-1].status in REDIRECTS or not isinstance(raw.body, bytes):
            raise ProtocolError('success requires final response and owned body bytes')
    elif (not isinstance(raw.failure, str) or not raw.failure.strip() or
          raw.body is not None):
        raise ProtocolError('failure requires reason and absent body')
    return sdk1.CompleteChain(raw.session_id, raw.request_id, tuple(hops),
                             raw.body, raw.failure)


def run(job_id, source, db):
    jobs = Jobs(db)
    url, attempt = jobs.pending(job_id)
    # A source callback or network wait never holds the local write transaction.
    try:
        raw = source.fetch(url)
    except sdk1.NetworkFailure as exc:
        raw = sdk1.CompleteChain(exc.session_id, exc.request_id,
                                exc.hops, None, exc.reason)
    result = _complete(raw, url)
    with db.atomic() as conn:
        if jobs.pending(job_id) != (url, attempt):
            raise InvalidState('attempt changed during acquisition')
        body = None
        if result.failure is None:
            body = Bodies(db).save(conn, f'b{job_id}.{attempt}', result.body)
        chain_id, state = Archive(db).publish(conn, job_id, attempt, result, body)
        jobs.finish(conn, job_id, attempt, chain_id, state)
    return {'chain_id': chain_id, 'state': state}


def capture(source, db):
    """Collect finite SDK1 targets; already terminal request keys are replayed.

    A recorded network failure is terminal until retry(job_id). Infrastructure
    errors leave the pending attempt unchanged and propagate to the caller.
    """
    jobs = Jobs(db)
    results = []
    for target in source.targets:
        job_id = jobs.enqueue(target.url, target.request_key)
        job = jobs.get_job(job_id)
        if job['state'] == 'pending':
            results.append(run(job_id, source, db))
        else:
            last = job['attempts'][-1]
            results.append({'chain_id': last['chain_id'], 'state': last['state']})
    return results
