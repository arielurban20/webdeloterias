import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parent
SCHEDULES_FILE = BASE_DIR / "draw_schedules.json"

# Reintentos después de la hora real del sorteo
RUN_WINDOWS = [2, 10, 25, 45]

# tolerancia por defecto para decidir si "toca correr ahora"
DEFAULT_MATCH_TOLERANCE_MINUTES = 3


@dataclass
class DueJob:
    game_slug: str
    state: str
    draw_type: str
    timezone: str
    draw_local: datetime
    trigger_local: datetime
    trigger_utc: datetime
    offset_minutes: int


def load_schedules() -> dict:
    with open(SCHEDULES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_hhmm(value: str) -> tuple[int, int]:
    hour, minute = value.split(":")
    return int(hour), int(minute)


def parse_test_now(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError("test-now debe incluir zona horaria, por ejemplo +00:00")
    return dt.astimezone(ZoneInfo("UTC"))


def build_draw_datetime(local_now: datetime, weekday: int, hhmm: str) -> datetime:
    hour, minute = parse_hhmm(hhmm)

    # Python weekday: Monday=0 Sunday=6
    # Nuestro JSON usa Sunday=0 ... Saturday=6
    py_target = (weekday - 1) % 7

    days_ahead = py_target - local_now.weekday()
    if days_ahead < 0:
        days_ahead += 7

    candidate_date = (local_now + timedelta(days=days_ahead)).date()
    return datetime(
        year=candidate_date.year,
        month=candidate_date.month,
        day=candidate_date.day,
        hour=hour,
        minute=minute,
        tzinfo=local_now.tzinfo,
    )


def iter_upcoming_jobs(now_utc: datetime, lookback_hours: int = 12, lookahead_hours: int = 24):
    schedules = load_schedules()
    utc_tz = ZoneInfo("UTC")

    for game_slug, game_data in schedules.items():
        tz_name = game_data["timezone"]
        state = game_data["state"]
        tz = ZoneInfo(tz_name)
        local_now = now_utc.astimezone(tz)

        for draw in game_data["draws"]:
            draw_type = draw["draw_type"]
            hhmm = draw["time"]

            for weekday in draw["days"]:
                draw_local = build_draw_datetime(local_now, weekday, hhmm)

                # revisar sorteo de esta semana y de la semana anterior
                # por si estamos en ventanas tardías
                for shift_days in (0, -7):
                    shifted_draw = draw_local + timedelta(days=shift_days)

                    delta_hours = (shifted_draw.astimezone(utc_tz) - now_utc).total_seconds() / 3600
                    if delta_hours < -lookback_hours or delta_hours > lookahead_hours:
                        continue

                    for offset in RUN_WINDOWS:
                        trigger_local = shifted_draw + timedelta(minutes=offset)
                        trigger_utc = trigger_local.astimezone(utc_tz)

                        yield DueJob(
                            game_slug=game_slug,
                            state=state,
                            draw_type=draw_type,
                            timezone=tz_name,
                            draw_local=shifted_draw,
                            trigger_local=trigger_local,
                            trigger_utc=trigger_utc,
                            offset_minutes=offset,
                        )


def find_due_jobs(
    now_utc: datetime | None = None,
    tolerance_minutes: int = DEFAULT_MATCH_TOLERANCE_MINUTES,
) -> list[DueJob]:
    if now_utc is None:
        now_utc = datetime.now(ZoneInfo("UTC"))

    due: list[DueJob] = []

    for job in iter_upcoming_jobs(now_utc):
        diff_minutes = abs((job.trigger_utc - now_utc).total_seconds()) / 60
        if diff_minutes <= tolerance_minutes:
            due.append(job)

    # evitar duplicados exactos
    seen = set()
    unique_due = []

    due.sort(key=lambda j: (j.trigger_utc, j.state, j.game_slug, j.offset_minutes))

    for job in due:
        key = (
            job.game_slug,
            job.state,
            job.draw_type,
            job.draw_local.isoformat(),
            job.offset_minutes,
        )
        if key in seen:
            continue
        seen.add(key)
        unique_due.append(job)

    return unique_due


def print_due_jobs(now_utc: datetime, tolerance_minutes: int):
    due = find_due_jobs(now_utc, tolerance_minutes=tolerance_minutes)

    print(f"NOW UTC: {now_utc.isoformat()}")
    print(f"TOLERANCE_MINUTES: {tolerance_minutes}")

    if not due:
        print("NO_DUE_JOBS")
        return

    for job in due:
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


def main():
    parser = argparse.ArgumentParser(description="Smart scheduler for lottery draw jobs")
    parser.add_argument(
        "--test-now",
        help='Hora manual para probar, ejemplo: "2026-04-10T18:05:00+00:00"',
    )
    parser.add_argument(
        "--tolerance",
        type=int,
        default=DEFAULT_MATCH_TOLERANCE_MINUTES,
        help="Tolerancia en minutos para considerar un job como due",
    )
    args = parser.parse_args()

    if args.test_now:
        now_utc = parse_test_now(args.test_now)
    else:
        now_utc = datetime.now(ZoneInfo("UTC"))

    print_due_jobs(now_utc, tolerance_minutes=args.tolerance)


if __name__ == "__main__":
    main()