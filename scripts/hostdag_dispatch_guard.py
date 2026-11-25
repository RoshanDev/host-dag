"""Revalidate a saved dispatch against its original ticket immediately before sending.

This is a receipt check, not a queue or an authorization source. Callers retain the
context captured when the work was approved; a newer ticket needs a new decision.
"""
from __future__ import annotations

from hostdag_json import public_path

import json
from pathlib import Path

import hostdag_ticket_state as ts


class DispatchError(Exception):
    def __init__(self, code, message):
        self.code, self.message = code, message
        super().__init__(message)


def read_object(path):
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("expected object")
        return value
    except (OSError, ValueError, TypeError) as exc:
        raise DispatchError("DISPATCH_UNVERIFIABLE", f"Cannot read dispatch authority: {path}") from exc


def capture(run_receipt, key, thread, turn_id):
    run_receipt = public_path(run_receipt)
    data = read_object(run_receipt)
    # Review keys are observation keys; the authority remains the original ticket.
    name = key.rsplit("-review-", 1)[0] if "-review-" in key else key
    matches = [(batch, body["tickets"][name]) for batch, body in (data.get("batches") or {}).items()
               if isinstance(body, dict) and name in (body.get("tickets") or {})]
    if len(matches) != 1:
        raise DispatchError("DISPATCH_UNVERIFIABLE", f"No unique ticket authority for {key}")
    batch, rec = matches[0]
    return {"run_receipt": str(run_receipt), "batch": batch, "ticket": name,
            "updated_at": rec.get("updated_at"), "thread": thread, "turn_id": turn_id}


def check(context, thread):
    for field in ("run_receipt", "batch", "ticket", "updated_at", "thread"):
        if not isinstance(context.get(field), str) or not context[field]:
            raise DispatchError("DISPATCH_UNVERIFIABLE", f"Dispatch context lacks {field}")
    if not Path(context["run_receipt"]).is_absolute() or context["thread"] != thread:
        raise DispatchError("DISPATCH_STALE", "Dispatch context belongs to another run/thread")
    data = read_object(context["run_receipt"])
    rec = ((data.get("batches") or {}).get(context["batch"]) or {}).get("tickets", {}).get(context["ticket"])
    if not isinstance(rec, dict):
        raise DispatchError("DISPATCH_STALE", "Ticket no longer exists")
    if rec.get("updated_at") != context["updated_at"]:
        raise DispatchError("DISPATCH_STALE", "Ticket updated_at changed after dispatch was saved")
    state = ts.derive_state(rec, ts.all_tickets(data))
    if state not in {ts.RUNNING, ts.REVIEWING} or rec.get("status") in {"ACCEPTED", "SUPERSEDED"}:
        raise DispatchError("DISPATCH_UNAUTHORIZED", f"Ticket is no longer active ({state})")
    sessions = {rec.get("executor")} if state == ts.RUNNING else {
        review.get("thread") for review in (rec.get("reviews") or {}).values()
        if isinstance(review, dict) and not review.get("verdict")}
    if thread not in sessions:
        raise DispatchError("DISPATCH_UNAUTHORIZED", "Thread no longer owns this execution/review")
    freeze_path = Path(context["run_receipt"]).parent / "control/locks/board-freeze.json"
    try:
        freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except (OSError, ValueError) as exc:
        raise DispatchError("DISPATCH_UNVERIFIABLE", "Cannot read board freeze") from exc
    if not isinstance(freeze, dict):
        raise DispatchError("DISPATCH_UNVERIFIABLE", "Invalid board freeze")
    # The class-based freeze policy is owned by the separate FREEZE-CLASS ticket.
    # Until that integration, retain the existing explicit exemption contract.
    if context["ticket"] not in (freeze.get("exempt") or []):
        raise DispatchError("DISPATCH_FROZEN", "Board freeze holds this dispatch")


# §16 review admission is a board transaction, never prompt interpretation.
# A reserved/uncertain token is not replayable. Known pre-submit failures may
# cancel their reservation; a lost mutation response must first be reconciled.
def _review_rows(run):
    return [(b, tid, t) for b, body in run.get('batches', {}).items()
            for tid, t in body.get('tickets', {}).items()]


def _aliases(tid, row):
    out = {'ticket:' + tid}
    for field, prefix in (('root_ticket', 'ticket:'), ('chain_id', 'chain:')):
        if row.get(field):
            out.add(prefix + str(row[field]))
    return out


def _critical(tid, row):
    from hostdag_dag_view import critical_tool
    return critical_tool(row) or (row.get('critical_path') is True
        and row.get('kind') not in ('product', 'doc', 'docs'))


def _review_scope(run, tid, row):
    """Count owned ticket/root/chain history; blocked_step is not an owner."""
    import copy
    import hashlib
    from hostdag_ticket_state import derive_state, REVIEWING, TERMINAL
    from board_freeze import freeze_stamp
    allrows = _review_rows(run)
    aliases = _aliases(tid, row)
    groups = (run.get('review_budget') or {}).get('groups', {})
    members, selected = [], {}
    while True:
        old = set(aliases)
        members = [(b, i, t) for b, i, t in allrows if aliases & _aliases(i, t)]
        for _, i, t in members:
            aliases.update(_aliases(i, t))
        if aliases == old:
            break
    protected = row.get('kind') not in ('product', 'doc', 'docs') and any(
        _critical(i, t) for _, i, t in members + [('', tid, row)])
    if not protected:
        return None
    if not any(row.get(k) for k in ('root_ticket', 'chain_id')) and not (row.get('freeze_exempt') or {}).get('blocked_step'):
        raise DispatchError('REVIEW_IDENTITY_REQUIRED', 'critical review needs root_ticket, chain_id or registered blocked_step')
    owners = {i for _, i, _ in members}
    # Old groups may span unrelated tickets through step aliases. Partition by
    # token ownership, keeping the original group for its other receipts.
    selected = {k: prev for k, prev in groups.items()
                if aliases & set(prev['aliases'])}
    gid = hashlib.sha256('|'.join(sorted(aliases)).encode()).hexdigest()[:24]
    if gid in selected:
        selected = {gid: selected[gid]}
    group_ids={k for k, prev in selected.items() if set(prev['aliases']) <= aliases}
    for key in list(group_ids):group_ids.update(selected[key].get('member_groups',[]))
    group_ids.add(gid)
    g = dict(id=gid,member_groups=sorted(group_ids),aliases=sorted(aliases), sources={}, cycles={}, tokens={})
    for prev in selected.values():
        owned_tokens = {k: copy.deepcopy(t) for k, t in prev.get('tokens', {}).items()
                        if t.get('ticket') in owners}
        g['tokens'].update(owned_tokens)
        for key, cycle in prev.get('cycles', {}).items():
            axes = {axis: token for axis, token in cycle['axes'].items()
                    if token in owned_tokens}
            if axes:
                g['cycles'][key] = dict(copy.deepcopy(cycle), axes=axes)
        for key, source in prev.get('sources', {}).items():
            if key.startswith('legacy-ticket:') and key[14:] in owners:
                saved = g['sources'].setdefault(key, dict(count=0, watermark=0))
                for field in ('count', 'watermark'):
                    saved[field] = max(saved[field], source[field])
    legacy_step_groups = list(selected.values())
    # Only wholly owned groups can be replaced; mixed legacy ledgers stay intact.
    selected = {k: prev for k, prev in selected.items()
                if set(prev['aliases']) <= aliases}
    for cycle in g['cycles'].values():
        g['first'] = min(g.get('first', cycle['at']), cycle['at'])
    floors = {}
    for _, i, t in members:
        policy = t
        accounted = (any(token.get('ticket') == i for token in g['tokens'].values()) or
                     any(r.get('source') and r.get('token') in g['tokens']
                         for r in (t.get('reviews') or {}).values()))
        if not accounted or any('legacy-step:' + str(t.get('review_step_accounted')) in prev.get('sources', {}) for prev in legacy_step_groups):
            # Already-accounted rows of the same explicit lineage may carry
            # copies of one old step high-water. Import that floor once.
            step = t.get('review_step_accounted')
            owner = min(j for _, j, other in members
                        if other.get('review_step_accounted') == step) if step else i
            source = 'legacy-ticket:' + owner
            floor = max([int(t.get('review_round_count') or 0),int(bool(t.get('reviews'))),
                        int(t.get('round') or 0) if derive_state(t) in TERMINAL | {REVIEWING} else 0]+
                        [int(e.get('round') or 0) for e in t.get('review_rounds',[])])
            if accounted:
                floor = max(0, floor - sum(not c.get('legacy') and any(
                    g['tokens'].get(token, {}).get('ticket') == i for token in c['axes'].values())
                    for c in g['cycles'].values()))
        else:
            source, floor = None, 0
        if source:
            floors[source] = max(floors.get(source, 0), floor)
        for field, key in (('first_reviewing_at','first'), ('review_pass_at','passed'), ('review_cap_trigger_at','failed_at')):
            if key == 'failed_at' and derive_state(t) in TERMINAL:
                continue  # closed receipts remain counted, but cannot fail a successor
            at = freeze_stamp(policy.get(field))
            if at is not None:
                g[key] = min(g.get(key, at), at)
        # Legacy current-cycle FAIL must not open a second axis after round 3.
        history=t.get('review_rounds',[])
        known_first=[freeze_stamp(e.get('at')) for e in history]+[freeze_stamp(r.get('registered_at')) for r in (t.get('reviews') or {}).values()]
        for at in known_first:
            if at is not None:g['first']=min(g.get('first',at),at)
        latest = max([freeze_stamp(e.get('at')) or 0 for e in history], default=0)
        reviews=list((t.get('reviews') or {}).values())
        if reviews and all(r.get('verdict')=='PASS' and (r.get('candidate') is None or r['candidate']==t.get('candidate')) and (r.get('round') is None or r['round']==t.get('round')) and freeze_stamp(r.get('registered_at')) is not None and freeze_stamp(r['registered_at'])>=latest for r in reviews):
            at=max(freeze_stamp(r['registered_at']) for r in reviews);g['passed']=min(g.get('passed',at),at)
        for r in (t.get('reviews') or {}).values():
            at = freeze_stamp(r.get('registered_at'))
            if (derive_state(t) not in TERMINAL and
                    int(policy.get('review_round_count') or t.get('review_round_count') or 0) >= 3 and r.get('verdict') == 'FAIL' and
                    ((at is None and not accounted) or (at is not None and at >= latest)) and
                    (r.get('candidate') is None or r['candidate'] == t.get('candidate'))):
                g['failed_at'] = min(g.get('failed_at', at or latest), at or latest)
    for key, floor in floors.items():
        source = g['sources'].setdefault(key, dict(count=0, watermark=0))
        source['count'] += max(0, floor - source['watermark'])
        source['watermark'] = max(floor, source['watermark'])
    g['count'] = sum(s['count'] for s in g['sources'].values()) + sum(
        not c.get('legacy') and bool(c['axes']) for c in g['cycles'].values())
    # Import an already-open legacy round once; its registered axes are occupied.
    if not g['cycles'] and g['count'] and derive_state(row) == REVIEWING:
        history = row.get('review_rounds') or []
        reviews = row.get('reviews') or {}
        if history or reviews:
            g['cycles']['legacy'] = dict(n=g['count'], candidate=row.get('candidate'),
                at=(history[-1].get('at') if history else g.get('first', 0)),
                axes={a:'legacy' for a in reviews}, legacy=True)
    g['counted_reviews'] = sorted({
        f"{b}/{i} thread={r.get('thread') or '<unrecorded>'}"
        for b, i, t in members for r in (t.get('reviews') or {}).values()
    } | {
        f"{t['batch']}/{t['ticket']} thread=" + str(
            t.get('receipt', {}).get('threadId') or t.get('receipt', {}).get('unit') or
            '<' + t['status'] + ':' + key + '>')
        for key, t in g['tokens'].items() if t.get('status') != 'retryable'
    } | {
        f"{b}/{i} thread=<legacy-unrecorded>"
        for b, i, t in members if not t.get('reviews') and
        g['sources'].get('legacy-ticket:' + i, {}).get('count', 0)
    })
    return gid, g, selected, members


def _review_cap(g, message):
    return DispatchError('REVIEW_CAP', message + '; counted reviews: ' +
                         ', '.join(g.get('counted_reviews', [])))


def review_policy_snapshot(run, tid, row):
    result = _review_scope(run, tid, row)
    if not result:
        return {}
    _, g, _, _ = result
    out = {'review_round_count':g['count'], 'review_budget_enforced':True}
    if g.get('first') is not None:
        out['first_reviewing_at'] = g['first']
    if g.get('passed') is not None:
        out['review_pass_at'] = g['passed']
    return out


def _resolve_review(root, run, ticket, batch, thread, required):
    import hostdag_dag_view as view
    matches = [(b,i,t,a) for b,i,t in _review_rows(run)
               for a,r in (t.get('reviews') or {}).items()
               if view.bare_executor(r.get('thread')) == view.bare_executor(thread)] if thread else []
    if not required and thread and not matches:
        executors = [(b, i, t) for b, i, t in _review_rows(run)
                     if (not ticket or i == ticket) and (batch is None or b == batch)
                     and view.bare_executor(t.get('executor')) == view.bare_executor(thread)]
        if executors:
            return None
    if ticket:
        ticket = ticket.rsplit('-review-',1)[0]
        matches = [(b,i,t,None) for b,i,t in _review_rows(run)
                   if i == ticket and (batch is None or b == batch)]
    if not matches and thread:
        for g in (run.get('review_budget') or {}).get('groups',{}).values():
            for t in g.get('tokens',{}).values():
                if t.get('receipt',{}).get('threadId')==thread:
                    found=_resolve_review(root,run,t['ticket'],t.get('batch'),None,True)
                    return (*found[:3],t['axis'])
        if any(view.bare_executor(t.get('executor')) == thread for _,_,t in _review_rows(run)):
            return None
        watch = read_object(root/'watch.json') if (root/'watch.json').exists() else {}
        if watch.get('parent_thread') == thread:
            return None
        for key, tracked in (watch.get('tracked') or {}).items():
            if tracked.get('thread') == thread and '-review-' in key:
                return _resolve_review(root, run, key, batch, None, True)
    if not matches:
        if not required and not (root/'run.json').exists():
            return None
        raise DispatchError('REVIEW_IDENTITY_REQUIRED','no registered ticket/reviewer identity')
    if len(matches) != 1:
        raise DispatchError('REVIEW_IDENTITY_REQUIRED','ambiguous ticket identity: supply batch')
    return matches[0]


def reserve_review(run_dir, *, ticket=None, batch=None, thread=None, axis='spec', token,
                   fingerprint, required=True, now=None):
    """Atomically reserve one call/axis. No board lock is held during host I/O."""
    import copy
    import time
    import hostdag_dag_view as view
    from board_freeze import freeze_gate_problem
    root = Path(run_dir); path=root/'run.json'
    if not path.exists():
        raise DispatchError('REVIEW_IDENTITY_REQUIRED','run.json required for DAG dispatch')
    now = time.time() if now is None else now
    if not token or axis not in ('spec','standards'):
        raise DispatchError('REVIEW_IDENTITY_REQUIRED','review token and axis required')
    with view.RunLock(path):
        mtime=path.stat().st_mtime;run=view.load_json(path)
        found=_resolve_review(root,run,ticket,batch,thread,required)
        if found is None:
            return None
        b,tid,row,registered_axis=found;axis=registered_axis or axis
        if view.ts.derive_state(row) in view.ts.TERMINAL | view.ts.DEGRADE_STATES | {view.ts.FROZEN_GATE}:
            raise DispatchError('DISPATCH_UNAUTHORIZED','closed/frozen ticket cannot start review')
        problem=freeze_gate_problem(root,run,tid,row,b)
        if problem:raise DispatchError('DISPATCH_FROZEN',problem)
        if not row.get('root_ticket') and not row.get('chain_id'):
            from board_freeze import release_steps
            step=(row.get('freeze_exempt') or {}).get('blocked_step')
            if step:
                try:registered=step in release_steps(root)
                except (OSError,ValueError) as exc:
                    raise DispatchError('REVIEW_IDENTITY_REQUIRED','fallback blocked_step registry missing/invalid') from exc
                if not registered:
                    raise DispatchError('REVIEW_IDENTITY_REQUIRED','fallback blocked_step is not registered')
        result=_review_scope(run,tid,row)
        if result is None:return None
        gid,g,old,members=result
        binding=dict(fingerprint=fingerprint,axis=axis,ticket=tid,batch=b,candidate=row.get('candidate'),thread=thread)
        previous=g['tokens'].get(token)
        if previous and any(previous.get(k)!=v for k,v in binding.items()):
            raise DispatchError('REVIEW_TOKEN_STALE','token reused with different request/candidate/identity')
        if previous and previous['status']=='delivered':
            return dict(previous, token=token, replay=True)
        if previous and previous['status'] in ('reserved','dispatching','uncertain'):
            raise DispatchError('REVIEW_INFLIGHT','reserved/uncertain token cannot be resubmitted')
        if g.get('first') is not None and now>=g['first']+21600 and (g.get('passed') is None or g['passed']>g['first']+21600):
            raise _review_cap(g, 'six hours without PASS; structured degradation required; original review path stopped')
        if g.get('failed_at') is not None and g['count']>=3:
            raise _review_cap(g, 'third-round FAIL; structured degradation required; original review path stopped')
        candidate=row.get('candidate')
        if not candidate:raise DispatchError('REVIEW_IDENTITY_REQUIRED','review candidate required')
        cycles=[(k,c) for k,c in g['cycles'].items() if c['axes'] or c.get('legacy')]
        current=max(cycles,key=lambda kv:(kv[1]['at'],kv[1]['n'])) if cycles else None
        join=current and current[1]['candidate']==candidate and axis not in current[1]['axes']
        if not join and g['count'] + 1 >= 3 and g.get('failed_at') is not None:
            raise _review_cap(g, 'third-round FAIL would refuse submit; reservation unchanged')
        if g['count']>3 or (not join and g['count']>=3):
            raise _review_cap(g, 'fourth review cycle refused; decision text is not an execution receipt')
        if join:cycle_id,cycle=current
        else:
            cycle_id=token;cycle=dict(n=g['count']+1,candidate=candidate,at=now,axes={})
            g['cycles'][cycle_id]=cycle;g['count']+=1
        cycle['axes'][axis]=token;g.setdefault('first',now)
        grant=dict(binding,group=gid,cycle=cycle_id,round=cycle['n'],status='reserved',at=now)
        g['tokens'][token]=grant
        # Preserve original step ledgers and mark their migration watermark. A
        # shared step high-water is imported once, never once per successor.
        for alias in g['aliases']:
            if alias.startswith('step:'):
                step=alias[5:];policy=run.setdefault('review_steps',{}).setdefault(step,{})
                policy['review_round_count']=max(int(policy.get('review_round_count') or 0),g['count'])
                policy['first_reviewing_at']=min(policy.get('first_reviewing_at',g['first']),g['first'])
                source=g['sources'].setdefault('legacy-step:'+step,dict(count=0,watermark=0))
                source['watermark']=max(source['watermark'],policy['review_round_count'])
        view.apply_state(row,view.ts.REVIEWING)
        row['review_round_count']=g['count'];row['first_reviewing_at']=g['first'];row['review_budget_group']=gid
        step=(row.get('freeze_exempt') or {}).get('blocked_step')
        if step:row['review_step_accounted']=step
        if not join:
            row.setdefault('review_rounds',[]).append(dict(round=cycle['n'],at=now,candidate=candidate,budget_cycle=cycle_id,ticket=tid))
        row.setdefault('review_dispatch_tokens',{})[axis]=token
        row['updated_at']=view.next_updated_at(row.get('updated_at'))
        ledger=run.setdefault('review_budget',{}).setdefault('groups',{})
        for old_id in old:ledger.pop(old_id,None)
        ledger[gid]=g;view.save_run(path,run,mtime)
        return dict(grant,token=token,replay=False)


def _canonical_review_token(run, token):
    """Resolve immutable identity first, then use only the owned ledger state.

    Mixed pre-migration ledgers retain stale copies for other tickets' receipts.
    Their status must never grant a second submission or override settlement.
    Conflicting identities or a missing canonical token cannot be reconciled here.
    """
    groups = (run.get('review_budget') or {}).get('groups', {})
    copies = [g['tokens'][token] for g in groups.values() if token in g.get('tokens', {})]
    if not copies:
        raise DispatchError('REVIEW_TOKEN_STALE', 'missing reservation')
    # A refunded request may reserve a later round or join another cycle.
    # Those fields belong to the canonical reservation, not request identity.
    identity = ('ticket', 'batch', 'candidate', 'axis', 'fingerprint')
    first = copies[0]
    if any(key not in t for t in copies for key in identity) or any(
            any(t[key] != first[key] for key in identity) for t in copies):
        raise DispatchError('REVIEW_TOKEN_STALE', 'conflicting reservation identity')
    row = run.get('batches', {}).get(first['batch'], {}).get('tickets', {}).get(first['ticket'])
    if not row:
        raise DispatchError('DISPATCH_STALE', 'review ticket missing')
    scope = _review_scope(run, first['ticket'], row)
    if scope is None:
        raise DispatchError('DISPATCH_STALE', 'review scope changed before submit')
    gid, g, old, _ = scope
    current = g['tokens'].get(token)
    if current is None:
        raise DispatchError('REVIEW_TOKEN_STALE', 'canonical reservation missing')
    if current.get('status') not in ('reserved', 'dispatching', 'uncertain', 'delivered', 'retryable'):
        raise DispatchError('REVIEW_INFLIGHT', 'canonical reservation state unknown')
    if current['status'] != 'retryable':
        cycle = g['cycles'].get(current.get('cycle'))
        if (not cycle or cycle.get('n') != current.get('round') or
                cycle.get('axes', {}).get(current['axis']) != token):
            raise DispatchError('REVIEW_TOKEN_STALE', 'canonical reservation cycle/round mismatch')
    if current['status'] == 'delivered' and not isinstance(current.get('receipt'), dict):
        raise DispatchError('REVIEW_TOKEN_STALE', 'delivered reservation receipt missing')
    current['group'] = gid
    return gid, g, old, row, current


def settle_review(run_dir, grant, status, receipt=None):
    """Known not-submitted is refundable; uncertain holds its slot until readback."""
    if not grant or not grant.get('token'):return
    import hostdag_dag_view as view
    path=Path(run_dir)/'run.json'
    with view.RunLock(path):
        mtime=path.stat().st_mtime;run=view.load_json(path)
        # Execution-only queue entries also carry observation tokens. With no
        # review reservation there is no budget to settle or submit authority.
        if not any(grant['token'] in g.get('tokens', {})
                   for g in (run.get('review_budget') or {}).get('groups', {}).values()):
            return
        gid,g,old,row,tok = _canonical_review_token(run, grant['token'])
        groups = run['review_budget']['groups']
        if tok['status']=='delivered' or tok['status']==status:return
        row['review_budget_group'] = gid
        tok['status']=status
        if receipt is not None:tok['receipt']=receipt
        if status=='retryable':
            c=g['cycles'][tok['cycle']]
            if c['axes'].get(tok['axis'])==grant['token']:c['axes'].pop(tok['axis'])
            if not c['axes'] and not c.get('legacy'):g['cycles'].pop(tok['cycle'])
            g['count']=sum(s['count'] for s in g['sources'].values())+sum(not c.get('legacy') and bool(c['axes']) for c in g['cycles'].values())
            # Refund only reservations, not historical/other-axis work.
            for _,_,row in _review_rows(run):
                if row.get('review_budget_group') in g.get('member_groups',[tok['group']]):
                    row['review_round_count']=g['count']
                    row['review_rounds']=[e for e in row.get('review_rounds',[]) if e.get('budget_cycle')!=tok['cycle'] or c['axes']]
            for alias in g['aliases']:
                if alias.startswith('step:'):
                    p=(run.get('review_steps') or {}).get(alias[5:])
                    if p:
                        p['review_round_count']=g['count']
                        g['sources']['legacy-step:'+alias[5:]]['watermark']=g['count']
            if not g['count'] and not any(c['axes'] for c in g['cycles'].values()):
                g.pop('first',None)
                for _,_,row in _review_rows(run):
                    if row.get('review_budget_group') in g.get('member_groups',[tok['group']]):row.pop('first_reviewing_at',None)
                for alias in g['aliases']:
                    if alias.startswith('step:'):(run.get('review_steps') or {}).get(alias[5:],{}).pop('first_reviewing_at',None)
        for old_id in old:groups.pop(old_id, None)
        groups[gid] = g
        view.save_run(path,run,mtime)



def review_fingerprint(thread, message, options=None):
    import hashlib
    return hashlib.sha256((thread+'\0'+message+('\0'+json.dumps(options,sort_keys=True) if options else '')).encode()).hexdigest()


def claim_review(run_dir, token, fingerprint):
    """One physical submit per token, including forwarding from the send queue."""
    import time
    import hostdag_dag_view as view
    path=Path(run_dir)/'run.json'
    with view.RunLock(path):
        mtime=path.stat().st_mtime;run=view.load_json(path)
        gid,g,old,row,t = _canonical_review_token(run, token)
        if t['fingerprint']!=fingerprint:raise DispatchError('REVIEW_TOKEN_STALE','message/identity changed')
        if t['status']=='delivered':return dict(t,token=token,replay=True)
        if t['status']!='reserved':raise DispatchError('REVIEW_INFLIGHT','token already submitted or uncertain')
        if row.get('candidate')!=t['candidate'] or view.ts.derive_state(row) in view.ts.TERMINAL | view.ts.DEGRADE_STATES | {view.ts.FROZEN_GATE}:
            raise DispatchError('DISPATCH_STALE','review ticket/candidate changed before submit')
        if g['count']>3 or (g['count']>=3 and g.get('failed_at') is not None):
            raise _review_cap(g, 'review cap reached before submit')
        from board_freeze import freeze_gate_problem
        problem=freeze_gate_problem(Path(run_dir),run,t['ticket'],row,t['batch'])
        if problem:raise DispatchError('DISPATCH_FROZEN',problem)
        if g.get('first') is not None and time.time()>=g['first']+21600 and (g.get('passed') is None or g['passed']>g['first']+21600):
            raise _review_cap(g, 'six-hour deadline crossed before submit')
        t['status']='dispatching'
        row['review_budget_group'] = gid
        ledger=run['review_budget']['groups']
        for key in old:ledger.pop(key,None)
        ledger[gid]=g
        view.save_run(path,run,mtime)
        return dict(t,token=token,replay=False)


def validate_review_registration(run, tid, fields, batch=None):
    """rv_register binds an existing dispatch; it cannot mint another allowance."""
    targets=[r for r in (fields.get('reviews') or {}).values() if r.get('thread')]
    source = fields.get('review_from')
    if source and (not targets or not fields.get('review_token')):
        raise DispatchError('REVIEW_TOKEN_REQUIRED', '--review-from needs reviews and a delivered review token')
    if not targets:return
    row=dict(next((t for b,i,t in _review_rows(run) if i==tid and (batch is None or b==batch)),{}))
    for key in ('root_ticket','chain_id','candidate'):
        if fields.get(key) is not None:row[key]=fields[key]
    result=_review_scope(run,tid,row)
    if result is None:
        if source:
            raise DispatchError('REVIEW_TOKEN_REQUIRED', '--review-from requires a shared review budget')
        return
    _,g,_,_=result
    token=fields.get('review_token')
    source_batch, source_tid = batch, tid
    if source:
        if not isinstance(source, str) or source.count('/') != 1:
            raise DispatchError('REVIEW_TOKEN_REQUIRED', '--review-from must be BATCH/TICKET')
        source_batch, source_tid = source.split('/')
        source_row = run.get('batches', {}).get(source_batch, {}).get('tickets', {}).get(source_tid)
        if not source_row or source_row.get('candidate') != row.get('candidate'):
            raise DispatchError('REVIEW_TOKEN_REQUIRED', 'review source missing or candidate differs')
    for axis,r in (fields.get('reviews') or {}).items():
        if not r.get('thread'):continue
        grant=g['tokens'].get(token) if token else next((t for t in g['tokens'].values() if t.get('ticket')==tid and t.get('axis')==axis and r['thread'] in (t.get('receipt',{}).get('threadId'),t.get('receipt',{}).get('unit'))),None)
        if source:
            canonical_gid, _, _, _, grant = _canonical_review_token(run, token)
            if canonical_gid != g['id']:
                raise DispatchError('REVIEW_TOKEN_REQUIRED', 'review source is outside the shared budget')
        if (not grant or grant['axis']!=axis or grant['ticket']!=source_tid or (source_batch is not None and grant['batch']!=source_batch)
                or grant['candidate']!=row.get('candidate') or grant['status']!='delivered'
                or r['thread'] not in (grant.get('receipt',{}).get('threadId'),grant.get('receipt',{}).get('unit'))):
            raise DispatchError('REVIEW_TOKEN_REQUIRED','rv_register requires the delivered dispatch token/receipt')
    return result


class ReviewReplay(Exception):
    def __init__(self,receipt):self.receipt=receipt


# ---- Review required-checks guard, fail closed (TOOL-GUARD-FAIL-CLOSED, Fable L2177 §4(a)) ----
# hostdag_review_required_checks runs in its own process group with a short deadline. A guard that
# is killed, times out, exits non-zero or prints anything but a well-formed selection refuses the
# dispatch with INFRA_ERROR before any review round, budget token or attempt is consumed; it is
# never a review FAIL (policy-v3: INVALID_TOOL != PRODUCT_FAIL).
REVIEW_GUARD_TIMEOUT = 20.0
REVIEW_GUARD_ARGV = None  # None: [sys.executable, <scripts>/hostdag_review_required_checks.py]


def _guard_refusal(detail):
    return DispatchError('INFRA_ERROR', 'review required-checks guard ' + detail +
                         '; dispatch refused (BLOCKED, not a review FAIL; no review round or attempt consumed)')


def _valid_selection(value, known_rules):
    def abs_dir(path):
        return isinstance(path, str) and Path(path).is_absolute() and Path(path).is_dir()
    def str_list(items):
        return isinstance(items, list) and all(isinstance(x, str) for x in items)
    if not isinstance(value, dict) or not abs_dir(value.get('repo')):
        return False
    roots, required = value.get('roots'), value.get('required')
    if not isinstance(roots, list) or not isinstance(required, list):
        return False
    if any(not isinstance(r, dict) or not abs_dir(r.get('repo')) for r in roots):
        return False
    return all(isinstance(r, dict) and r.get('id') in known_rules and isinstance(r.get('reason'), str)
               and isinstance(r.get('why'), str) and str_list(r.get('hits')) and str_list(r.get('commands'))
               and r['commands'] and (r.get('repo') is None or abs_dir(r.get('repo'))) for r in required)


def review_required_checks(prompt, cwd, base=None, ticket=None, reasons=None):
    """(prompt + required-checks section, selection), or DispatchError INFRA_ERROR. Never fails open."""
    import contextlib
    import os
    import signal
    import subprocess
    import sys
    import tempfile
    import hostdag_review_required_checks as rc
    script = Path(__file__).resolve().with_name('hostdag_review_required_checks.py')
    with tempfile.TemporaryDirectory(prefix='host-dag-review-guard-') as tmp:
        contract = Path(tmp) / 'contract.md'
        contract.write_text(prompt)
        argv = [*(REVIEW_GUARD_ARGV or [sys.executable, str(script)]), 'select', '--cwd', str(cwd),
                '--contract', str(contract), *(['--base', base] if base else [])]
        try:
            proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True, start_new_session=True)
        except OSError as exc:
            raise _guard_refusal('could not start: ' + str(exc)) from exc
        try:
            out, err = proc.communicate(timeout=REVIEW_GUARD_TIMEOUT)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.communicate()
            raise _guard_refusal(f'timed out after {REVIEW_GUARD_TIMEOUT:g}s') from None
    if proc.returncode < 0:
        raise _guard_refusal(f'killed by signal {-proc.returncode}')
    if proc.returncode:
        raise _guard_refusal(f'exited {proc.returncode}: {(err or "").strip()[-300:]}')
    try:
        selection = json.loads(out)
    except ValueError:
        selection = None
    if not _valid_selection(selection, {r['id'] for r in rc.RULES}):
        raise _guard_refusal('returned unparseable output: ' + repr((out or '')[:200]))
    if reasons:
        selection['review_reasons'] = list(reasons)
    return prompt + rc.contract_section(selection, ticket), selection
