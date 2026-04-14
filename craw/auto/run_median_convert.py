"""
Script chạy full convert Data Median qua HTTP API.
Tự động chain qua 4 scopes: ward → region → region_total → project.
"""
import requests
import time
import sys

BASE_URL = "http://localhost:89/data-median/convert-batch"
LIMIT = 500
TIMEOUT = 600  # 10 phút mỗi batch

def main():
    scope = sys.argv[1] if len(sys.argv) > 1 else "ward"
    offset = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    total = 0
    batch_num = 0

    print(f"=== FULL CONVERT DATA MEDIAN ===")
    print(f"Start: {time.strftime('%H:%M:%S')}")
    print(f"scope={scope}, offset={offset}, limit={LIMIT}")
    print()

    while True:
        batch_num += 1
        t0 = time.time()
        try:
            resp = requests.post(BASE_URL, data={
                "scope": scope,
                "offset": offset,
                "limit": LIMIT,
            }, timeout=TIMEOUT)
            data = resp.json()
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ❌ Loi: {e}")
            print(f"  Retry sau 5s... (scope={scope}, offset={offset})")
            time.sleep(5)
            continue

        elapsed = time.time() - t0
        processed = data.get("processed", 0)
        total += processed
        done_scope = data.get("done_scope", False)
        next_scope = data.get("next_scope", scope)
        next_offset = data.get("next_offset", offset)
        done = data.get("done", False)

        print(f"[{time.strftime('%H:%M:%S')}] #{batch_num} scope={scope} offset={offset} "
              f"processed={processed} total={total} ({elapsed:.1f}s)")

        if done:
            print()
            print(f"=== ✅ HOAN THANH! Tong: {total} ===")
            print(f"End: {time.strftime('%H:%M:%S')}")
            break

        if done_scope and next_scope != scope:
            print(f"  → Chuyen sang scope: {next_scope}")

        scope = next_scope
        offset = next_offset

if __name__ == "__main__":
    main()
