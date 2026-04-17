import json
import os
import subprocess
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from smart_scheduler import find_due_jobs

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
LOCK_DIR = BASE_DIR / "scheduler_locks"
LOCK_DIR.mkdir(exist_ok=True)

PYTHON_EXE = sys.executable


def make_lock_name(state: str, game_slug: str, draw_type: str, draw_date: str, offset_minutes: int) -> Path:
    safe_draw_type = draw_type.replace("/", "-").replace(" ", "-").lower()
    return LOCK_DIR / f"{state}__{game_slug}__{safe_draw_type}__{draw_date}__{offset_minutes}.lock"


def already_ran_job(job) -> bool:
    draw_date = job.draw_local.date().isoformat()
    lock_file = make_lock_name(
        state=job.state,
        game_slug=job.game_slug,
        draw_type=job.draw_type,
        draw_date=draw_date,
        offset_minutes=job.offset_minutes,
    )

    if lock_file.exists():
        return True

    payload = {
        "state": job.state,
        "game_slug": job.game_slug,
        "draw_type": job.draw_type,
        "draw_date": draw_date,
        "offset_minutes": job.offset_minutes,
        "trigger_utc": job.trigger_utc.isoformat(),
        "created_at_utc": datetime.now(ZoneInfo("UTC")).isoformat(),
    }
    lock_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return False


def cleanup_old_locks(days: int = 3):
    cutoff = datetime.now(ZoneInfo("UTC")) - timedelta(days=days)
    for path in LOCK_DIR.glob("*.lock"):
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=ZoneInfo("UTC"))
            if mtime < cutoff:
                path.unlink(missing_ok=True)
        except Exception:
            pass


def build_command_for_state(state: str) -> list[str]:
    return [
        PYTHON_EXE,
        str(BASE_DIR / "scrape_all_states_dom_v6.py"),
        "--state",
        state,
    ]


def run_command(cmd: list[str]) -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)

    print("RUNNING:", " ".join(cmd))
    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env,
    )
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Run scheduled lottery scrapers with retry windows")
    parser.add_argument("--test-now", help="ISO datetime override, e.g. 2026-04-11T01:05:00+00:00")
    args = parser.parse_args()

    if args.test_now:
        now_utc = datetime.fromisoformat(args.test_now)
    else:
        now_utc = datetime.now(ZoneInfo("UTC"))

    cleanup_old_locks()

    due_jobs = find_due_jobs(now_utc)

    if not due_jobs:
        print("NO_DUE_JOBS")
        return

    print("DUE_JOBS:")
    for job in due_jobs:
        print(
            json.dumps(
                {
                    "game_slug": job.game_slug,
                    "state": job.state,
                    "draw_type": job.draw_type,
                    "timezone": job.timezone,
                    "draw_local": job.draw_local.isoformat(),
                    "trigger_local": job.trigger_local.isoformat(),
                    "trigger_utc": job.trigger_utc.isoformat(),
                    "offset_minutes": job.offset_minutes,
                },
                ensure_ascii=False,
            )
        )

    # Agrupamos por estado solo para ejecutar 1 scrape por estado por pasada,
    # pero sin impedir que otro offset futuro vuelva a intentarlo si hace falta.
    states_to_run = []
    seen_states = set()

    for job in due_jobs:
        if already_ran_job(job):
            print(
                f"SKIP_DUPLICATE_JOB: state={job.state} game={job.game_slug} "
                f"draw_type={job.draw_type} offset={job.offset_minutes}"
            )
            continue

        if job.state not in seen_states:
            seen_states.add(job.state)
            states_to_run.append(job.state)

    if not states_to_run:
        print("NO_STATES_TO_RUN_AFTER_LOCKS")
        return

    final_exit = 0

    for state in states_to_run:
        cmd = build_command_for_state(state)
        exit_code = run_command(cmd)

        if exit_code != 0:
            print(f"STATE_RUN_FAILED: {state} -> exit_code={exit_code}")
            final_exit = exit_code
        else:
            print(f"STATE_RUN_OK: {state}")

    sys.exit(final_exit)


if __name__ == "__main__":
    main()