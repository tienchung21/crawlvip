import argparse
import sys
import time
from datetime import datetime
from typing import Dict, Optional

import requests


DEFAULT_BASE_URL = "http://127.0.0.1:89"
DEFAULT_ENDPOINT = "/data-median/convert-batch"
DEFAULT_LIMIT = 1000
DEFAULT_RUN_DAY = 29


def call_convert_batch(
    base_url: str,
    endpoint: str,
    scope: str,
    offset: int,
    limit: int,
    month: str,
    timeout_sec: int,
) -> Dict:
    url = base_url.rstrip("/") + endpoint
    payload = {
        "scope": scope,
        "offset": offset,
        "limit": limit,
        "month": month,
    }
    resp = requests.post(url, data=payload, timeout=timeout_sec)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Invalid JSON response: {data}")
    return data


def run_monthly_convert(
    base_url: str,
    endpoint: str,
    month: str,
    limit: int,
    timeout_sec: int,
    sleep_sec: float,
    max_loops: int,
) -> None:
    scope = "ward"
    offset = 0
    loops = 0
    total_processed = 0

    print(f"=== MONTHLY DATA_MEDIAN START | month={month} ===")
    print(f"base_url={base_url} endpoint={endpoint} limit={limit}")

    while True:
        loops += 1
        if max_loops > 0 and loops > max_loops:
            raise RuntimeError(f"Exceeded max_loops={max_loops}, aborting to avoid infinite loop.")

        data = call_convert_batch(
            base_url=base_url,
            endpoint=endpoint,
            scope=scope,
            offset=offset,
            limit=limit,
            month=month,
            timeout_sec=timeout_sec,
        )

        processed = int(data.get("processed", 0) or 0)
        done_scope = bool(data.get("done_scope", False))
        next_scope = str(data.get("next_scope", scope))
        next_offset = int(data.get("next_offset", offset) or 0)
        done = bool(data.get("done", False))

        total_processed += processed
        print(
            f"[loop={loops}] scope={scope} offset={offset} processed={processed} "
            f"done_scope={done_scope} next_scope={next_scope} next_offset={next_offset} done={done}"
        )

        if done:
            break

        # Same chaining logic as frontend demo page.
        if done_scope and next_scope != scope:
            scope = next_scope
            offset = next_offset
        else:
            scope = next_scope
            offset = next_offset

        if sleep_sec > 0:
            time.sleep(sleep_sec)

    print(f"=== MONTHLY DATA_MEDIAN DONE | month={month} total_processed={total_processed} loops={loops} ===")


def main():
    parser = argparse.ArgumentParser(
        description="Auto monthly data_median calculator. Runs on day 29 by default."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Laravel base URL")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Convert endpoint path")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Batch limit per call")
    parser.add_argument("--run-day", type=int, default=DEFAULT_RUN_DAY, help="Run day of month (default 29)")
    parser.add_argument("--month", default="", help="Target month YYYY-MM (default: current month)")
    parser.add_argument("--force", action="store_true", help="Run regardless of current day")
    parser.add_argument("--timeout-sec", type=int, default=180, help="HTTP timeout seconds")
    parser.add_argument("--sleep-sec", type=float, default=0.05, help="Sleep between batch calls")
    parser.add_argument("--max-loops", type=int, default=50000, help="Safety max loops")
    args = parser.parse_args()

    now = datetime.now()
    target_month = args.month.strip() if args.month.strip() else now.strftime("%Y-%m")

    if not args.force and now.day != int(args.run_day):
        print(
            f"Skip run: today={now.strftime('%Y-%m-%d')} day={now.day}, "
            f"configured run_day={args.run_day}."
        )
        return

    try:
        run_monthly_convert(
            base_url=args.base_url,
            endpoint=args.endpoint,
            month=target_month,
            limit=max(50, min(args.limit, 2000)),
            timeout_sec=max(30, args.timeout_sec),
            sleep_sec=max(0.0, args.sleep_sec),
            max_loops=max(1, args.max_loops),
        )
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
