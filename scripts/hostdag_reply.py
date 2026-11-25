"""Explicit decision-reply identity shared by the host and its existing queue."""
import re


def add_arguments(parser):
    parser.add_argument('--steer-if-busy', action='store_true', default=False)
    parser.add_argument('--reply-to')
    parser.add_argument('--decision-version', type=int)
    parser.add_argument('--ticket', help='ticket identity for the decision marker')


def identity(args):
    if not args.steer_if_busy:
        if args.reply_to is not None or args.decision_version is not None:
            raise ValueError('--reply-to/--decision-version require --steer-if-busy')
        return None
    ticket = args.ticket or getattr(args, 'guard_ticket', None)
    if not args.reply_to or not args.decision_version or args.decision_version < 1 or not ticket:
        raise ValueError('--steer-if-busy requires --reply-to, positive --decision-version and --ticket')
    if any(not re.fullmatch(r'[A-Za-z0-9_.:/-]+', value) for value in (args.reply_to, ticket)):
        raise ValueError('reply and ticket identities must be single marker-safe tokens')
    return {'question_id': args.reply_to, 'decision_version': args.decision_version, 'ticket': ticket}


def mark(message, reply):
    if not reply:
        return message
    marker = '[reply-to {question_id} decision-v{decision_version} ticket={ticket}]'.format(**reply)
    return message if message.splitlines()[0] == marker else marker + '\n' + message


def accepted(method, turn_id):
    if method not in {'start', 'steer'} or not isinstance(turn_id, str) or not turn_id:
        raise ValueError('accepted reply needs a method and nonempty turnId')
    return {'status': 'ACCEPTED', 'method': method, 'turnId': turn_id}
