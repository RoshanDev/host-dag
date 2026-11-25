"""Audited narrow mutations for host-view ticket set; never a second board writer.

All callers hold the existing RunLock. Intent includes exact before/after and a
CAS rollback, so a crash between the board commit and receipt is recoverable.
"""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import sys
import time
import uuid

import hostdag_dag_view as view


def lint_module():
    spec = importlib.util.spec_from_file_location('lint_actions_reader', Path(__file__).with_name('host-dag-lint.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def append(path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as stream:
        stream.write('\n' + json.dumps(row, ensure_ascii=False) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def commit(run_path, run, mtime, batch, tid, before, after, rule, cli_argv=None):
    lint = lint_module()
    ident = 'lint-auto-' + uuid.uuid4().hex
    command = [sys.executable, str(Path(__file__).with_name('host-dag-view.py')),
               '--run', str(run_path.parent), 'ticket', 'set', '--batch', batch, '--ticket', tid,
               '--rollback-lint-action', ident, '--expected-revision', lint.digest(after)]
    row = dict(id=ident, at=time.time(), batch=batch, ticket=tid, rule=rule,
               before=before, after=after, action=rule, cli_argv=cli_argv or [sys.executable, *sys.argv],
               rollback_command=command, rollback=shlex.join(command))
    audit = run_path.parent / 'control/lint-auto-actions.jsonl'
    append(audit, dict(row, result='intent'))
    view.save_run(run_path, run, mtime)
    append(audit, dict(row, result='applied'))
    return row


def provisional_plan(root, run, batch, tid, registration_path, lint):
    """Read one explicit parent grant, never discover grants or promote finality.

    Called only inside the existing run lock. The receipt authorizes this one
    mechanical action; it is not authority for the decision's business steps.
    """
    root = root.resolve()

    def local_file(value):
        if not isinstance(value, str) or not Path(value).is_absolute():
            raise ValueError('registration and decision paths must be absolute')
        path = Path(value).resolve(strict=True)
        if not path.is_relative_to(root) or not path.is_file() or path.stat().st_size >= lint.LARGE:
            raise ValueError('registration and decision files must belong to this run')
        return path

    path = local_file(registration_path)
    raw_registration = path.read_bytes()
    registration = json.loads(raw_registration)
    if not isinstance(registration, dict):
        raise ValueError('parent registration must be an object')
    registered_run = registration.get('run')
    if not isinstance(registered_run, str) or not Path(registered_run).is_absolute() or Path(registered_run).resolve() != root:
        raise ValueError('parent registration run mismatch')
    parent = view.load_json(root / 'watch.json').get('parent_thread')
    if not isinstance(parent, str) or not parent or registration.get('parent_thread') != parent:
        raise ValueError('parent registration parent_thread mismatch')
    if registration.get('authority') != 'coordinator-to-gpt-1145' or registration.get('provisional') is not True:
        raise ValueError('parent registration is not the authorized provisional contract')
    key = registration.get('key')
    if not isinstance(key, str) or not key:
        raise ValueError('parent registration needs an exact decision key')
    actions = registration.get('automatic_actions')
    action_fields = {'action', 'batch', 'ticket', 'expected_revision', 'wait_on'}
    if not isinstance(actions, list) or not actions or any(
            not isinstance(a, dict) or set(a) != action_fields or
            any(not isinstance(v, str) or not v for v in a.values()) or
            a['action'] != 'wait-on-decided' or a['wait_on'] != 'decision:' + key for a in actions):
        raise ValueError('invalid or missing automatic_actions permission')
    grants = [a for a in actions if a['batch'] == batch and a['ticket'] == tid]
    if len(grants) != 1:
        raise ValueError('parent registration must grant exactly this target once')
    matches = [(b, t) for b, body in run.get('batches', {}).items()
               for name, t in body.get('tickets', {}).items() if name == tid]
    if len(matches) != 1 or matches[0][0] != batch or not isinstance(matches[0][1], dict):
        raise ValueError('target ticket missing or ambiguous')
    ticket = matches[0][1]
    if (view.ts.derive_state(ticket, view.all_tickets(run)) not in view.ts.WAITING or
            ticket.get('page_request') or ticket.get('wait_on') != grants[0]['wait_on'] or
            lint.digest(ticket) != grants[0]['expected_revision']):
        raise ValueError('target wait or revision changed')
    source = local_file(registration.get('decision_file'))
    raw_decision = source.read_bytes()
    if hashlib.sha256(raw_decision).hexdigest() != registration.get('sha256'):
        raise ValueError('decision file hash mismatch')
    decision = json.loads(raw_decision)
    latest = view.advisor_answers(root).get(key)  # exact latest key, no suffix alias
    # JSON identity must distinguish true from 1 (Python dict equality does not).
    if not isinstance(decision, dict) or not isinstance(latest, dict) or lint.digest(decision) != lint.digest(latest):
        raise ValueError('latest advisor record differs from bound decision file')
    view.validate_record(decision)
    at = registration.get('decision_at')
    if (type(at) not in (int, float) or at != decision['at'] or decision.get('key') != key or
            decision.get('authority') != registration['authority'] or
            decision.get('provisional') is not True or decision.get('event') == 'l1-sample' or
            decision.get('verdict') != 'decided' or decision.get('fallback_safe') is not True):
        raise ValueError('decision is not the exact safe, decided provisional authorized by parent')
    pending = {'kind': 'apply_decision', 'key': key, 'decision_at': at, 'provisional': True,
               'authority': registration['authority'], 'parent_registration': str(path),
               'parent_registration_sha256': hashlib.sha256(raw_registration).hexdigest(),
               'decision_file': str(source), 'decision_sha256': registration['sha256']}
    state = view.ts.WAITING_DEPENDENCY if view.ts.dependency_status(ticket, view.all_tickets(run))[0] else view.ts.CREATED
    return {'fields': {'state': state, 'wait_on': '', 'wait_holder': '',
                       'blocker': f'父已绑定可执行暂行（provisional），仅解除精确等待；登记：{path}；原说明：{ticket.get("blocker") or ""}'},
            'pending_action': pending, 'revision': grants[0]['expected_revision']}


def execute(run_path, batch, tid, rule, revision, rollback=None, parent_registration=None):
    lint = lint_module()
    with view.RunLock(run_path):
        mtime = run_path.stat().st_mtime
        run = view.load_json(run_path)
        before = copy.deepcopy(run.get('batches', {}).get(batch, {}).get('tickets', {}).get(tid))
        result = dict(ticket=tid, batch=batch, expected_revision=revision, result='skipped')
        if parent_registration is not None and (rule != 'wait-on-decided' or rollback):
            return dict(result, reason='parent registration only permits wait-on-decided')
        if tid in lint.core_tickets(run) and rule not in (*view.freeze_gate.FREEZE_RULES, DEGRADE_RULE):
            return dict(result, reason="core-chain: parent only")
        if rollback:
            rows = lint.json_rows(run_path.parent / 'control/lint-auto-actions.jsonl')
            row = next((r for r in reversed(rows) if r.get('id') == rollback and r.get('batch') == batch and r.get('ticket') == tid), None)
            if not row or before != row.get('after') or lint.digest(before) != revision:
                return dict(result, reason='rollback revision changed or audit not found')
            after = copy.deepcopy(row['before'])
            run['batches'][batch]['tickets'][tid] = after
            receipt = commit(run_path, run, mtime, batch, tid, before, after, 'rollback:' + row['rule'])
        else:
            if parent_registration is not None:
                try:
                    plan = provisional_plan(run_path.parent, run, batch, tid, parent_registration, lint)
                except (OSError, ValueError, view.ViewError) as exc:
                    return dict(result, reason=str(exc))
            else:
                plan = lint.auto_plan(run_path.parent, run, batch, tid, rule)
                if plan and rule == 'wait-on-decided' and plan['proof']['answer'].get('tier') != 'L1':
                    return dict(result, reason='only final L1 mechanical decisions are automatic')
            if not plan or plan['revision'] != revision:
                return dict(result, reason='condition or revision changed')
            if rule == DEGRADE_RULE:
                receipt=start_degrade(run_path,run,mtime,batch,tid,before,plan['proof'],rule)
                return dict(result,result='applied',action_id=receipt['id'],rollback=receipt['rollback'])
            if rule == 'freeze-gate-clear':
                marker = run_path.parent / 'control/freeze-gate'
                # Recomputed plan includes exact marker and bound current receipt.
                append(run_path.parent / 'control/lint-auto-actions.jsonl',
                       dict(result='intent', rule=rule, before=plan['proof']['marker'],
                            after=None, proof=plan['proof'], at=time.time()))
                marker.unlink()
                append(run_path.parent / 'control/lint-auto-actions.jsonl',
                       dict(result='applied', rule=rule, before=plan['proof']['marker'],
                            after=None, proof=plan['proof'], at=time.time()))
                return dict(result, result='applied', action_id=rule)
            if rule == 'freeze-gate-stale-exempt':
                # This approved mechanical freeze changes no candidate/scope. Old
                # tool rows need not re-prove source classification to be stopped.
                after = copy.deepcopy(before)
                view.apply_state(after, view.ts.FROZEN_GATE)
                after.pop('pending_action', None)
                after.update(freeze_reason='stale_exempt', blocker=plan['fields']['blocker'],
                             updated_at=view.next_updated_at(before.get('updated_at')))
                run['batches'][batch]['tickets'][tid] = after
            else:
                after = view.apply_ticket_update(run, batch, tid, copy.deepcopy(plan['fields']), run_dir=run_path.parent)
            if plan['pending_action']:
                after['pending_action'] = plan['pending_action']
            receipt = commit(run_path, run, mtime, batch, tid, before, after, rule)
        return dict(result, result='applied', action_id=receipt['id'], rollback=receipt['rollback'])


# policy16-degrade-exec: board validation, bounded background execution, one
# existing CAS writer. No external shell text is evaluated.
DEGRADE_RULE = 'review-cap-degrade-due'
DEGRADE_HOOKS_FILE = 'control/degrade-hooks.json'


def degrade_hooks(root):
    """Project-declared hard-hook entries for degrade option b.

    Read from $HOST_DAG_DEGRADE_HOOKS or <run>/control/degrade-hooks.json:
      {"entries_root": "/abs/dir (default: this scripts dir)",
       "entries": {"tools/release.py": {"positionals": ["package"],
                   "options": {"--run": 1, "--dry-run": 0}, "run_flag": "--run"}},
       "heads_repos": ["APP"],      # inputs keys that must match control/release-heads.json
       "materials": ["manifest"],   # inputs keys hashed into the proof
       "lock": "local-heavy-admission"}
    No file means no hard hooks: option b is refused, a/c/NONE still work.
    """
    path = Path(os.environ.get('HOST_DAG_DEGRADE_HOOKS') or Path(root) / DEGRADE_HOOKS_FILE)
    if not path.is_file():
        return {'entries_root': Path(__file__).parent.resolve(), 'entries': {},
                'heads_repos': [], 'materials': [], 'lock': 'local-heavy-admission'}
    raw = json.loads(path.read_text())
    entries = {}
    for rel, spec in (raw.get('entries') or {}).items():
        options = spec.get('options') or {}
        if not isinstance(options, dict) or any(v not in (0, 1) for v in options.values()):
            raise ValueError('degrade hook options must map option name to arity 0/1: ' + rel)
        run_flag = spec.get('run_flag') or '--run'
        if run_flag not in options:
            raise ValueError('degrade hook run_flag must be a declared option: ' + rel)
        entries[rel] = {'positionals': tuple(spec.get('positionals') or ()),
                        'options': dict(options), 'run_flag': run_flag}
    return {'entries_root': Path(raw.get('entries_root') or Path(__file__).parent).resolve(),
            'entries': entries, 'heads_repos': list(raw.get('heads_repos') or []),
            'materials': list(raw.get('materials') or []),
            'lock': raw.get('lock') or 'local-heavy-admission'}


def degrade_argv_options(spec, entry, argv):
    positionals,options=spec['positionals'],spec['options']
    args=argv[2:] if argv[:2]==[sys.executable,str(entry)] else argv[1:]
    # Check every token, even one where a caller intended a value. --flag=value
    # compares the full option name, never its prefix; short aliases and -- are
    # excluded. Help/version/write-heads also cannot bypass the hard hook.
    for token in args:
        if token.startswith('-') and token.partition('=')[0] not in options:
            raise ValueError('argv contains a gate-skipping or undeclared option: '+token)
    parsed={};bare=[];i=0
    while i<len(args):
        token=args[i];i+=1
        if not token.startswith('-'):
            bare.append(token);continue
        name,equal,value=token.partition('=');arity=options[name]
        if name in parsed:raise ValueError('duplicate argv option: '+name)
        if not arity:
            if equal:raise ValueError('boolean argv option cannot have =value: '+name)
            parsed[name]=True
        else:
            if not equal:
                if i==len(args) or args[i].startswith('-'):raise ValueError('argv option value required: '+name)
                value=args[i];i+=1
            if not value:raise ValueError('empty argv option value: '+name)
            parsed[name]=value
    if tuple(bare)!=positionals:raise ValueError('argv must select the declared hard-hook action')
    return parsed


def degrade_schema(root, ticket):
    """Return normalized plan + material-bound proof, or a precise invalid reason.

    NONE is intentionally minimal. Non-NONE input references are bound both to
    this candidate and the current release-heads vector, never a copied old SHA.
    """
    import re
    import math
    root=Path(root)
    plan=ticket.get('degrade_plan')
    if not isinstance(plan,dict) or plan.get('option') not in ('a','b','c','NONE'):
        raise ValueError('option must be a/b/c/NONE; free-text fallback_ref is invalid')
    if 'fallback_ref' in plan:raise ValueError('fallback_ref is obsolete')
    option=plan['option']
    if option!='b' and plan.get('argv') not in (None,[], 'NONE'):
        raise ValueError('argv is only allowed for option=b')
    if not plan.get('removal_disposition'):raise ValueError('removal_disposition required')
    if option=='NONE':
        if not plan.get('reason'):raise ValueError('NONE requires reason')
        return copy.deepcopy(plan), {'option':'NONE'}
    for key in ('idempotency_key','receipt_path','inputs','timeout'):
        if not plan.get(key):raise ValueError(key+' required')
    if not isinstance(plan['idempotency_key'],str):raise ValueError('idempotency_key must be text')
    if type(plan['timeout']) not in (int,float) or not math.isfinite(plan['timeout']) or plan['timeout']<=0:
        raise ValueError('timeout must be positive seconds')
    def file(value,exists=True):
        if not isinstance(value,str) or not Path(value).is_absolute():raise ValueError('absolute file path required')
        path=Path(value).resolve()
        if exists and not path.is_file():raise ValueError('missing input '+str(path))
        return path
    file(plan['receipt_path'],False)
    vector=json.loads(file(plan['inputs']).read_text())
    hooks=degrade_hooks(root)
    heads=(json.loads((root/'control/release-heads.json').read_text()).get('heads',{})
           if hooks['heads_repos'] else {})
    if vector.get('candidate')!=ticket.get('candidate'):raise ValueError('inputs candidate mismatch')
    for repo in hooks['heads_repos']:
        sha=vector.get(repo)
        if not isinstance(sha,str) or not re.fullmatch('[0-9a-f]{40}',sha) or heads.get(repo)!=sha:
            raise ValueError(repo+' input must match current release-heads')
        declared=re.search(r'(?:^|,)'+repo+r':([0-9a-f]{40})(?:,|$)',str(ticket.get('candidate') or ''))
        if declared and declared[1]!=sha:raise ValueError(repo+' candidate mismatch')
    material={key:str(file(vector.get(key))) for key in hooks['materials']}
    proof={'option':option,'vector':vector,'materials':{k:hashlib.sha256(Path(v).read_bytes()).hexdigest() for k,v in material.items()}}
    if option=='a':
        if not isinstance(plan.get('narrow_fix_scope'),str) or not plan['narrow_fix_scope'].strip():raise ValueError('a requires narrow_fix_scope')
    if option=='c':
        if plan.get('coverage_loss') is not False or not isinstance(plan.get('sole_path_for'),list):
            raise ValueError('c requires coverage_loss=false and sole_path_for declaration')
        if plan['sole_path_for'] or ticket.get('three_direction_unique_path'):
            raise ValueError('c forbidden for three-direction sole verification/fix path')
        baseline=json.loads(file(plan.get('coverage_baseline')).read_text())
        if baseline.get('status')!='PASS' or baseline.get('candidate')!=ticket.get('candidate'):
            raise ValueError('c requires a current coverage-gate PASS baseline')
        proof['coverage_baseline']=baseline
    if option=='b':
        argv=plan.get('argv');entry=file(plan.get('entry'))
        allowed={hooks['entries_root'].joinpath(p).resolve():s for p,s in hooks['entries'].items()}
        if not allowed:raise ValueError('b requires hard-hook entries declared in '+DEGRADE_HOOKS_FILE)
        if entry not in allowed:raise ValueError('entry must be a declared hard-hook entry')
        spec=allowed[entry]
        if not isinstance(argv,list) or not argv or any(not isinstance(a,str) or not a for a in argv):raise ValueError('b requires argv array')
        prefix=[sys.executable,str(entry)]
        if argv[:2]!=prefix and argv[:1]!=[str(entry)]:raise ValueError('argv must directly invoke entry; no shell/interpreter flags')
        parsed=degrade_argv_options(spec,entry,argv)
        flag=spec['run_flag']
        if flag not in parsed or Path(parsed[flag]).resolve()!=root.resolve():
            raise ValueError('hard hook must bind current run')
        lock=plan.get('required_lock')
        if not isinstance(lock,dict) or lock.get('name')!=hooks['lock']:raise ValueError('b requires lock '+hooks['lock'])
        for key in ('expected_min','max_min','wait_seconds'):
            if type(lock.get(key)) not in (int,float) or not math.isfinite(lock[key]) or lock[key]<=0:raise ValueError('lock '+key+' must be positive')
        if lock['max_min']<lock['expected_min']:raise ValueError('lock max_min below expected_min')
        proof['entry_sha256']=hashlib.sha256(entry.read_bytes()).hexdigest()
    return copy.deepcopy(plan),proof


def degrade_proposal(root,run,tid,ticket,now=None):
    """Pure deterministic proposal. Legacy invalid plans become NONE at trigger."""
    from hostdag_dispatch_guard import review_policy_snapshot
    now=time.time() if now is None else now
    if view.ts.derive_state(ticket) in view.ts.TERMINAL | view.ts.DEGRADE_STATES | {view.ts.FROZEN_GATE}:
        return None
    if not view.critical_tool(ticket) or tid=='TOOL-POLICY-16-REVIEW-CAP' or ticket.get('degrade_execution'):return None
    policy={**ticket,**view.step_review_policy(run,ticket)}
    if ticket.get('root_ticket') or ticket.get('chain_id') or ticket.get('review_budget_group'):
        policy.update(review_policy_snapshot(run,tid,ticket))
    at,trigger=view.review_cap_trigger(policy)
    if at is None or now<at:return None
    try:plan,proof=degrade_schema(root,ticket);invalid=None
    except (ValueError,OSError,TypeError,KeyError) as exc:
        plan={'option':'NONE','reason':'invalid plan','removal_disposition':'candidate blocked'};proof={};invalid=str(exc)
    ident=hashlib.sha256(json.dumps([ticket.get('review_budget_group') or ticket.get('root_ticket') or ticket.get('chain_id') or (ticket.get('freeze_exempt') or {}).get('blocked_step') or tid,ticket.get('candidate'),at,plan.get('idempotency_key')],sort_keys=True).encode()).hexdigest()[:24]
    return dict(option=plan['option'],plan=plan,material=proof,invalid=invalid,trigger=trigger,trigger_at=at,id=ident)


def degrade_receipt_path(root,proposal):
    return Path(proposal['plan'].get('receipt_path') or Path(root)/'control/degrade'/proposal['id']/'receipt.json')


def start_degrade(run_path,run,mtime,batch,tid,before,proposal,rule):
    """Caller holds RunLock; commit before the bounded asynchronous local start."""
    import subprocess
    from hostdag_json import atomic_json
    root=run_path.parent;ident=proposal['id'];option=proposal['option']
    receipt_path=degrade_receipt_path(root,proposal);receipt_path.parent.mkdir(parents=True,exist_ok=True)
    after=copy.deepcopy(before)
    after['degrade_decision']={'option':option,'trigger':proposal['trigger'],'at':time.time()}
    after['degrade_execution']={'id':ident,'option':option,'receipt_path':str(receipt_path),'candidate':before.get('candidate')}
    view.apply_state(after,view.ts.DEGRADING)
    unit='host-dag-degrade-'+ident
    state=view.ts.CANDIDATE_BLOCKED
    if option=='a':
        contract=receipt_path.with_name('narrow-fix-contract.md')
        contract.write_text(proposal['plan']['narrow_fix_scope']+'\n')
        after['degrade_execution']['contract']=str(contract);state=view.ts.READY_DEGRADE_A
    if option!='b':
        view.apply_state(after,state)
        after['blocker']='待协调者：换方法继续，或本候选不发布、流水线继续；不得转 r4/已知限制'
        after['degrade_execution']['status']='CONTRACT_READY' if option=='a' else 'BLOCKED'
        atomic_json(receipt_path,dict(after['degrade_execution'],trigger=proposal['trigger'],invalid=proposal['invalid'],at=time.time()))
    else:
        after['degrade_execution'].update(status='QUEUED',unit=unit)
        after['blocker']='降级排队等待 heavy；unit='+unit
    after['updated_at']=view.next_updated_at(before.get('updated_at'))
    run['batches'][batch]['tickets'][tid]=after
    audit=commit(run_path,run,mtime,batch,tid,before,after,rule)
    if option=='b':
        request=receipt_path.with_name(ident+'-request.json')
        atomic_json(request,dict(run=str(root),batch=batch,ticket=tid,proposal=proposal,revision=lint_module().digest(after)))
        # The outer worker holds no lock while waiting. host-dag-lock-run is invoked
        # inside it and starts the argv unit only after the lock is admitted.
        lock=proposal['plan']['required_lock']
        command=['/usr/bin/systemd-run','--user','--collect','--unit='+unit,'--property=MemoryMax=8G',
                 '--property=RuntimeMaxSec='+str(lock['wait_seconds']+proposal['plan']['timeout']+90),
                 sys.executable,str(Path(__file__).resolve()),'--degrade-work',str(request)]
        try:
            started=subprocess.run(command,capture_output=True,text=True,timeout=10)
            if started.returncode:raise RuntimeError(started.stderr[:1000])
        except (OSError,RuntimeError,subprocess.TimeoutExpired) as exc:
            # Start outcome can be unknown: never resubmit another unit. A receipt
            # and lint finding make this visible to the coordinator.
            atomic_json(receipt_path,dict(after['degrade_execution'],status='START_UNCONFIRMED',error=str(exc),at=time.time()))
    return audit


def degrade_work(request_path):
    """Unit supervisor: lock admission, argv unit, receipt, original CLI CAS."""
    import subprocess
    from hostdag_json import atomic_json
    request=json.loads(Path(request_path).read_text());root=Path(request['run']);p=request['proposal'];plan=p['plan']
    receipt=degrade_receipt_path(root,p);claim=receipt.with_name(p['id']+'-claim')
    try:
        with claim.open('x') as stream:stream.write(str(os.getpid()))
    except FileExistsError:return 75  # unknown prior execution is never replayed
    code=1;error=None
    try:
        ticket=view.load_json(root/'run.json')['batches'][request['batch']]['tickets'][request['ticket']]
        if lint_module().digest(ticket)!=request['revision']:raise ValueError('board revision changed before execution')
        current_plan,proof=degrade_schema(root,ticket)
        if current_plan!=plan:raise ValueError('execution contract changed before execution')
        if proof!=p['material']:raise ValueError('bound inputs/entry changed before execution')
        lock=plan['required_lock'];unit='host-dag-degrade-argv-'+p['id']
        argv=[sys.executable,str(Path(__file__).with_name('host-dag-lock-run')),'local-heavy-admission',
              '--run-dir',str(root),'--ticket',request['ticket'],'--phase','degrade',
              '--expected-min',str(lock['expected_min']),'--max-min',str(lock['max_min']),'-w',str(lock['wait_seconds']),
              '--unit','host-dag-degrade-'+p['id'],'--','/usr/bin/systemd-run','--user','--wait','--collect',
              '--unit='+unit,'--property=MemoryMax=8G','--property=RuntimeMaxSec='+str(plan['timeout']),*plan['argv']]
        code=subprocess.run(argv,timeout=lock['wait_seconds']+plan['timeout']+30).returncode
        if code<0:code=128-code
    except subprocess.TimeoutExpired:code=124;error='lock/argv timeout'
    except (OSError,ValueError,KeyError,TypeError) as exc:error=str(exc)
    payload=dict(id=p['id'],candidate=ticket.get('candidate') if 'ticket' in locals() else None,
                 option='b',status='PASS' if code==0 else 'FAIL',exit_code=code,error=error,at=time.time())
    atomic_json(receipt,payload)
    command=[sys.executable,str(Path(__file__).with_name('host-dag-view.py')),
             '--run',str(root),'ticket','set','--batch',request['batch'],'--ticket',request['ticket'],
             '--degrade-result',str(receipt),'--expected-revision',request['revision']]
    try:
        returned=subprocess.run(command,capture_output=True,text=True,timeout=30)
        atomic_json(receipt.with_name(p['id']+'-board-write.json'),{'exit_code':returned.returncode,'stdout':returned.stdout,'stderr':returned.stderr})
        if returned.returncode and code==0:code=1
    except (OSError,subprocess.TimeoutExpired):
        if code==0:code=1
    return code


def finish_degrade(run_path,batch,tid,receipt_path,revision):
    with view.RunLock(run_path):
        mtime=run_path.stat().st_mtime;run=view.load_json(run_path)
        t=run['batches'][batch]['tickets'][tid];before=copy.deepcopy(t);execution=t.get('degrade_execution') or {}
        data=json.loads(Path(receipt_path).read_text())
        if (lint_module().digest(t)!=revision or view.ts.derive_state(t)!=view.ts.DEGRADING or
            Path(receipt_path).resolve()!=Path(execution.get('receipt_path','')).resolve() or
            data.get('id')!=execution.get('id') or data.get('candidate')!=t.get('candidate') or
            data.get('status') not in ('PASS','FAIL') or type(data.get('exit_code')) is not int or
            (data['status']=='PASS')!=(data['exit_code']==0)):
            raise ValueError('degrade receipt/revision mismatch')
        t['degrade_execution'].update(data)
        view.apply_state(t,view.ts.WAITING_DEPENDENCY if data['status']=='PASS' else view.ts.CANDIDATE_BLOCKED)
        t['blocker']='待协调者验收降级执行回执：'+str(receipt_path) if data['status']=='PASS' else '降级失败，本候选阻断；换方法继续或本候选不发布'
        t['updated_at']=view.next_updated_at(t.get('updated_at'))
        audit=commit(run_path,run,mtime,batch,tid,before,t,'degrade-result')
        return {'ticket':tid,'result':'applied','action_id':audit['id'],'state':t['state']}


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--degrade-work',required=True)
    sys.exit(degrade_work(parser.parse_args().degrade_work))
