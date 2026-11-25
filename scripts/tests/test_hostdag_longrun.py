"""host-dag-longrun: receipts, real exit codes, one board write per attempt, replay, ORPHANED (Fable L3378 §1 (2)(3)).

Hermetic: a fake systemd-run runs the unit inline (main process, then ExecStopPost), a fake host-dag only logs
its argv, and the run directory is a temp dir. No real unit or board is touched."""
import json
import os
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
SH = HERE / "host-dag-longrun.sh"
sys.path.insert(0, str(HERE))
import hostdag_longrun as lr  # noqa: E402

FAKE_SYSTEMD_RUN = textwrap.dedent("""\
    #!/usr/bin/env python3
    # Records argv, then runs the command after -- inline with --setenv applied, then ExecStopPost.
    import json, os, shlex, subprocess, sys
    args = sys.argv[1:]
    with open(os.environ["FAKE_SRUN_LOG"], "a") as f:
        f.write(json.dumps(args) + "\\n")
    if os.environ.get("FAKE_SRUN_FAIL"):
        print("Failed to start transient service unit", file=sys.stderr)
        sys.exit(1)
    cut = args.index("--")
    env = dict(os.environ)
    post = None
    for a in args[:cut]:
        if a.startswith("--setenv="):
            k, _, v = a[len("--setenv="):].partition("=")
            env[k] = v
        if a.startswith("--property=ExecStopPost="):
            post = shlex.split(a[len("--property=ExecStopPost="):])
    rc = subprocess.run(args[cut + 1:], env=env).returncode
    if post and not os.environ.get("FAKE_SRUN_NO_POST"):
        env.update(EXIT_CODE="exited" if rc >= 0 else "killed",
                   EXIT_STATUS=str(rc) if rc >= 0 else str(-rc), SERVICE_RESULT="x")
        subprocess.run(post, env=env)
    """)

FAKE_HOST_DAG = textwrap.dedent("""\
    #!/usr/bin/env python3
    # One log line per board write (newlines inside the blocker escaped as \\n).
    import os, sys
    with open(os.environ["FAKE_HD_LOG"], "a") as f:
        f.write(" ".join(sys.argv[1:]).replace("\\n", "\\\\n") + "\\n")
    """)


def make_exec(path: Path, body: str) -> Path:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


class LongrunTest(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.root = Path(self.td.name)
        self.run_dir = self.root / "run"
        self.run_dir.mkdir()
        (self.run_dir / "run.json").write_text(json.dumps(
            {"batches": {"OLD": {"tickets": {"T-1": {"status": "DONE"}}},
                         "B1": {"tickets": {"T-1": {"status": "CLAIMED", "blocker": "prior note"}}}}}))
        self.srun = make_exec(self.root / "systemd-run", FAKE_SYSTEMD_RUN)
        self.hd = make_exec(self.root / "host-dag", FAKE_HOST_DAG)
        self.srun_log = self.root / "srun.log"
        self.hd_log = self.root / "hd.log"
        self.env = {**os.environ, "FAKE_SRUN_LOG": str(self.srun_log), "FAKE_HD_LOG": str(self.hd_log)}

    def tearDown(self):
        self.td.cleanup()

    def start(self, *cmd, label="job", attempt=None, extra=(), env=None):
        argv = [str(SH), "--ticket", "T-1", "--build", "b22", "--label", label, "--run", str(self.run_dir),
                "--systemd-run", str(self.srun), "--board-cmd", str(self.hd), *extra]
        if attempt is not None:
            argv += ["--attempt", str(attempt)]
        p = subprocess.run(argv + ["--", *cmd], capture_output=True, text=True, env={**self.env, **(env or {})},
                           cwd=self.root, timeout=60)
        out = json.loads(p.stdout.strip().splitlines()[-1])
        return p.returncode, out

    def board_lines(self):
        return self.hd_log.read_text().splitlines() if self.hd_log.exists() else []

    def srun_calls(self):
        return [json.loads(x) for x in self.srun_log.read_text().splitlines()] if self.srun_log.exists() else []

    def test_success_receipt_and_board(self):
        rc, out = self.start("/bin/sh", "-c", "echo out; echo err >&2; test -n \"$K1\"",
                             extra=["--setenv", "K1=secretvalue42", "--memory-max", "8G"])
        self.assertEqual(rc, 0)
        self.assertEqual(out["unit"], "host-dag-longrun-T-1-job-1")
        self.assertEqual(out["attempt"], 1)
        d = Path(out["receipt_dir"])
        self.assertTrue(d.name.startswith("job-1-"))
        self.assertEqual(d.parent, self.run_dir / "evidence/longrun/T-1")
        self.assertEqual((d / "exit_code").read_text(), "0\n")
        self.assertEqual((d / "stdout.log").read_text(), "out\n")
        self.assertEqual((d / "stderr.log").read_text(), "err\n")
        s = json.loads((d / "started.json").read_text())
        self.assertEqual(s["env_keys"], ["K1"])
        self.assertNotIn("secretvalue42", json.dumps(s))
        self.assertEqual(s["argv"][0], "/bin/sh")
        call = self.srun_calls()[0]
        uid = os.getuid()
        self.assertIn("--user", call)
        self.assertIn(f"--setenv=XDG_RUNTIME_DIR=/run/user/{uid}", call)
        self.assertIn(f"--setenv=DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{uid}/bus", call)
        self.assertIn("--property=MemoryMax=8G", call)
        self.assertTrue(all(Path(x).is_absolute() for x in call[call.index("--") + 1:][:2]))
        lines = self.board_lines()
        self.assertEqual(len(lines), 1)  # _run wrote; ExecStopPost saw board.lock and skipped
        self.assertIn("ticket set --batch B1 --ticket T-1 --blocker LONGRUN-DONE ticket=T-1 build=b22 attempt=1", lines[0])
        self.assertIn("exit=0", lines[0])
        self.assertIn(f"receipt={d}", lines[0])
        self.assertIn("\\nprior note", lines[0])  # whole old blocker on the next line
        self.assertNotIn("out", lines[0].split("receipt=")[0])  # no log content on the board
        self.assertEqual(json.loads((d / "board.json").read_text())["status"], "WRITTEN")

    def test_failure_exit_3(self):
        rc, out = self.start("/bin/sh", "-c", "exit 3")
        self.assertEqual(rc, 0)  # launcher returns at once; the job's code is in the receipt
        d = Path(out["receipt_dir"])
        self.assertEqual((d / "exit_code").read_text(), "3\n")
        self.assertIn("exit=3", self.board_lines()[0])
        p = subprocess.run([str(SH), "check", "--receipt", str(d)], capture_output=True, text=True)
        self.assertEqual(json.loads(p.stdout)["state"], "DONE")
        self.assertEqual(json.loads(p.stdout)["exit_code"], 3)

    def test_signal_exit_is_128_plus_n(self):
        rc, out = self.start("/bin/sh", "-c", "kill -TERM $$")
        d = Path(out["receipt_dir"])
        self.assertEqual((d / "exit_code").read_text(), "143\n")
        self.assertEqual(json.loads((d / "exit.json").read_text())["signal"], 15)
        self.assertIn("exit=143", self.board_lines()[0])

    def test_execstoppost_records_when_wrapper_died(self):
        d = self.run_dir / "evidence/longrun/T-1/x-1-s"
        d.mkdir(parents=True)
        (d / "started.json").write_text(json.dumps({"ticket": "T-1", "build": "b", "label": "x", "attempt": 1,
                                                    "unit": "u", "run": str(self.run_dir),
                                                    "board_cmd": [str(self.hd)]}))
        env = {**self.env, "EXIT_CODE": "killed", "EXIT_STATUS": "KILL"}
        subprocess.run([sys.executable, str(HERE / "hostdag_longrun.py"), "_finalize", "--receipt", str(d)],
                       env=env, check=True)
        self.assertEqual((d / "exit_code").read_text(), "137\n")
        self.assertEqual(len(self.board_lines()), 1)

    def test_replay_same_attempt_no_second_unit_one_board_line(self):
        rc1, first = self.start("/bin/true", attempt=7)
        rc2, again = self.start("/bin/true", attempt=7)
        self.assertEqual((rc1, rc2), (0, 0))
        self.assertTrue(again["replay"])
        self.assertFalse(again["started"])
        self.assertTrue(again["has_started_json"])
        self.assertEqual(again["receipt_dir"], first["receipt_dir"])
        self.assertEqual(again["unit"], first["unit"])
        self.assertEqual(len(self.srun_calls()), 1)
        self.assertEqual(len(self.board_lines()), 1)
        self.assertEqual(len(list((self.run_dir / "evidence/longrun/T-1").glob("job-7-*"))), 1)
        # a second notify on the same attempt (e.g. ExecStopPost after _run) is a no-op
        self.assertEqual(lr.notify_board(Path(first["receipt_dir"]))["status"], "ALREADY_NOTIFIED")
        self.assertEqual(len(self.board_lines()), 1)

    def test_auto_attempt_increments(self):
        _, a = self.start("/bin/true")
        _, b = self.start("/bin/true")
        self.assertEqual((a["attempt"], b["attempt"]), (1, 2))
        self.assertNotEqual(a["receipt_dir"], b["receipt_dir"])
        self.assertEqual(len(self.board_lines()), 2)

    def test_orphaned_when_unit_gone_without_exit_code(self):
        _, out = self.start("/bin/true", env={"FAKE_SRUN_NO_POST": "1"})
        d = Path(out["receipt_dir"])
        os.remove(d / "exit_code")  # simulate: wrapper killed before it could record, no ExecStopPost
        fake_ctl = make_exec(self.root / "systemctl", "#!/bin/sh\necho inactive\nexit 3\n")
        p = subprocess.run([str(SH), "check", "--receipt", str(d), "--systemctl", str(fake_ctl)],
                           capture_output=True, text=True)
        self.assertEqual(p.returncode, 4)
        self.assertEqual(json.loads(p.stdout)["state"], "ORPHANED")
        live_ctl = make_exec(self.root / "systemctl-active", "#!/bin/sh\necho active\n")
        p = subprocess.run([str(SH), "check", "--receipt", str(d), "--systemctl", str(live_ctl)],
                           capture_output=True, text=True)
        self.assertEqual(json.loads(p.stdout)["state"], "RUNNING")

    def test_launch_failure_reported(self):
        rc, out = self.start("/bin/true", env={"FAKE_SRUN_FAIL": "1"})
        self.assertEqual(rc, 2)
        self.assertEqual(out["error"], "LAUNCH_FAILED")
        self.assertTrue((Path(out["receipt_dir"]) / "launch_failed.json").exists())
        self.assertEqual(self.board_lines(), [])

    def test_relative_command_rejected(self):
        rc, out = self.start("true")
        self.assertEqual(rc, 2)
        self.assertIn("absolute", out["error"])
        self.assertEqual(self.srun_calls(), [])

    def test_bad_setenv_rejected(self):
        rc, out = self.start("/bin/true", extra=["--setenv", "NOEQUALS"])
        self.assertEqual(rc, 2)
        self.assertEqual(self.srun_calls(), [])

    def test_concurrent_board_writes_keep_both_lines(self):
        # fake host-dag that really rewrites the blocker in run.json, slowly, to widen the race window
        writer = make_exec(self.root / "host-dag-write", textwrap.dedent("""\
            #!/usr/bin/env python3
            import json, sys, time
            a = sys.argv[1:]
            run = a[a.index("--run") + 1]
            batch, ticket, blocker = a[a.index("--batch") + 1], a[a.index("--ticket") + 1], a[a.index("--blocker") + 1]
            p = run + "/run.json"
            data = json.load(open(p))
            time.sleep(0.3)
            data["batches"][batch]["tickets"][ticket]["blocker"] = blocker
            json.dump(data, open(p, "w"))
            """))
        receipts = []
        for label in ("a", "b"):
            d = self.run_dir / f"evidence/longrun/T-1/{label}-1-s"
            d.mkdir(parents=True)
            (d / "started.json").write_text(json.dumps({"ticket": "T-1", "build": "b", "label": label, "attempt": 1,
                                                        "unit": "u", "run": str(self.run_dir),
                                                        "board_cmd": [str(writer)]}))
            (d / "exit_code").write_text("0\n")
            receipts.append(d)
        procs = [subprocess.Popen([sys.executable, str(HERE / "hostdag_longrun.py"), "_finalize", "--receipt", str(d)])
                 for d in receipts]
        self.assertEqual([p.wait(timeout=30) for p in procs], [0, 0])
        blocker = json.loads((self.run_dir / "run.json").read_text())["batches"]["B1"]["tickets"]["T-1"]["blocker"]
        self.assertIn("label=a", blocker)
        self.assertIn("label=b", blocker)
        self.assertIn("prior note", blocker)

    def _three_writes_on_real_blocker(self, original):
        """Board writer that really rewrites run.json; finalize three attempts in turn and return the blocker."""
        data = json.loads((self.run_dir / "run.json").read_text())
        data["batches"]["B1"]["tickets"]["T-1"]["blocker"] = original
        (self.run_dir / "run.json").write_text(json.dumps(data))
        writer = make_exec(self.root / "host-dag-write", textwrap.dedent("""\
            #!/usr/bin/env python3
            import json, sys
            a = sys.argv[1:]
            p = a[a.index("--run") + 1] + "/run.json"
            data = json.load(open(p))
            data["batches"][a[a.index("--batch") + 1]]["tickets"][a[a.index("--ticket") + 1]]["blocker"] = \
                a[a.index("--blocker") + 1]
            json.dump(data, open(p, "w"))
            """))
        for n in (1, 2, 3):
            d = self.run_dir / f"evidence/longrun/T-1/long-{n}-s"
            d.mkdir(parents=True)
            (d / "started.json").write_text(json.dumps({"ticket": "T-1", "build": "b", "label": "long", "attempt": n,
                                                        "unit": "u", "run": str(self.run_dir),
                                                        "board_cmd": [str(writer)]}))
            (d / "exit_code").write_text("0\n")
            self.assertEqual(lr.notify_board(d)["status"], "WRITTEN")
        return json.loads((self.run_dir / "run.json").read_text())["batches"]["B1"]["tickets"]["T-1"]["blocker"]

    def test_long_blocker_with_freeze_exempt_kept_whole(self):
        original = ("FREEZE-EXEMPT: user-directed 11-20 test\n" + "派工说明 " + "X" * 5000 + "\nTAIL-MARKER-END")
        self.assertGreater(len(original), 5000)
        blocker = self._three_writes_on_real_blocker(original)
        lines = blocker.split("\n")
        self.assertEqual(lines[0], "FREEZE-EXEMPT: user-directed 11-20 test")
        self.assertTrue(lines[1].startswith("LONGRUN-DONE ") and "attempt=3" in lines[1])  # newest second
        self.assertTrue(blocker.endswith("TAIL-MARKER-END"))
        self.assertEqual(blocker.count("X" * 5000), 1)
        self.assertEqual(sum(x.startswith("LONGRUN-DONE ") for x in lines), 3)
        # removing the LONGRUN-DONE lines gives back the original byte for byte
        self.assertEqual("\n".join(x for x in lines if not x.startswith("LONGRUN-DONE ")), original)

    def test_long_blocker_without_freeze_exempt_kept_whole(self):
        original = "12:xx 父：等待条件 " + "Y" * 5000 + "\n第二行\nTAIL-MARKER-END"
        blocker = self._three_writes_on_real_blocker(original)
        lines = blocker.split("\n")
        self.assertTrue(lines[0].startswith("LONGRUN-DONE ") and "attempt=3" in lines[0])
        self.assertTrue(blocker.endswith(original))
        self.assertEqual(sum(x.startswith("LONGRUN-DONE ") for x in lines), 3)
        self.assertEqual("\n".join(x for x in lines if not x.startswith("LONGRUN-DONE ")), original)

    def test_no_batch_recorded_not_fatal(self):
        (self.run_dir / "run.json").write_text(json.dumps({"batches": {}}))
        _, out = self.start("/bin/true")
        d = Path(out["receipt_dir"])
        self.assertEqual((d / "exit_code").read_text(), "0\n")
        self.assertEqual(json.loads((d / "board.json").read_text())["status"], "NO_BATCH")
        self.assertEqual(self.board_lines(), [])


if __name__ == "__main__":
    unittest.main()
