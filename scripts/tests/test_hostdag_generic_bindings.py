"""Public defaults carry no project host names or release scripts."""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import hostdag_leftover_ledger as ledger  # noqa: E402
import hostdag_lint_actions as actions  # noqa: E402

spec = importlib.util.spec_from_file_location('board_freeze', SCRIPTS / 'local-dev/board_freeze.py')
bf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bf)


class LeftoverHostTest(unittest.TestCase):
    def test_host_tag_wins(self):
        self.assertEqual(ledger.leftover_host('vm on host:build-1 (scratch)'), 'build-1')

    def test_env_hosts_and_default_review(self):
        with mock.patch.dict(os.environ, {'HOST_DAG_LEFTOVER_HOSTS': 'alpha, beta'}):
            self.assertEqual(ledger.leftover_host('disk on beta'), 'beta')
        with mock.patch.dict(os.environ, {'HOST_DAG_LEFTOVER_HOSTS': ''}):
            self.assertEqual(ledger.leftover_host('disk on 10.0.0.5'), 'host needs review')


class FreezeEnvironmentTest(unittest.TestCase):
    def test_any_label_environment_separates(self):
        result = {'classes': ['local-build'], 'reason': 'declared'}
        marker = {'conflict_classes': ['local-build'], 'environment': 'staging'}
        with mock.patch.object(bf, 'decision', side_effect=lambda v, *a: {'classes': v[0]}), \
             mock.patch.object(bf, 'canonical_lock', side_effect=lambda x: x):
            self.assertFalse(bf.freeze_blocks(result, marker, {'environment': 'prod-a'}))
            self.assertTrue(bf.freeze_blocks(result, marker, {'environment': 'staging'}))


class DegradeHooksTest(unittest.TestCase):
    def test_no_config_means_no_hard_hooks(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(os.environ, {'HOST_DAG_DEGRADE_HOOKS': ''}):
            hooks = actions.degrade_hooks(root)
            self.assertEqual(hooks['entries'], {})
            self.assertEqual(hooks['heads_repos'], [])

    def test_declared_entry_parses_and_validates_argv(self):
        with tempfile.TemporaryDirectory() as root:
            entry = Path(root) / 'tools/release.py'
            entry.parent.mkdir()
            entry.write_text('')
            (Path(root) / 'control').mkdir()
            (Path(root) / 'control/degrade-hooks.json').write_text(json.dumps({
                'entries_root': root,
                'entries': {'tools/release.py': {'positionals': ['package'],
                                                 'options': {'--run': 1, '--dry-run': 0}}}}))
            with mock.patch.dict(os.environ, {'HOST_DAG_DEGRADE_HOOKS': ''}):
                hooks = actions.degrade_hooks(root)
            spec = hooks['entries']['tools/release.py']
            self.assertEqual(spec['run_flag'], '--run')
            parsed = actions.degrade_argv_options(spec, entry, [str(entry), 'package', '--run', root, '--dry-run'])
            self.assertEqual(parsed, {'--run': root, '--dry-run': True})
            with self.assertRaises(ValueError):
                actions.degrade_argv_options(spec, entry, [str(entry), 'package', '--skip-gate'])

    def test_bad_arity_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            cfg = Path(root) / 'hooks.json'
            cfg.write_text(json.dumps({'entries': {'x.py': {'options': {'--run': 2}}}}))
            with mock.patch.dict(os.environ, {'HOST_DAG_DEGRADE_HOOKS': str(cfg)}):
                with self.assertRaises(ValueError):
                    actions.degrade_hooks(root)


if __name__ == '__main__':
    unittest.main()
