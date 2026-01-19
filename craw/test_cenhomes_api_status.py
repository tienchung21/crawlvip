
import json
import time
from typing import Dict

import requests

BASE = "https://api-v3.cenhomes.vn/location/v1"


def _headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://cenhomes.vn",
        "referer": "https://cenhomes.vn/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
    }


def _post(path: str, payload: Dict) -> requests.Response:
    return requests.post(f"{BASE}/{path}", json=payload, headers=_headers(), timeout=20)


def _print_result(name: str, resp: requests.Response):
    status = resp.status_code
    try:
        data = resp.json()
    except Exception:
        data = None
    print(f"{name}: status={status}")
    if data is None:
        print("  body: <non-json>")
        return None
    err = data.get("error") if isinstance(data, dict) else None
    if err:
        print(f"  error: {err}")
    payload = data.get("data") if isinstance(data, dict) else None
    if isinstance(payload, list):
        print(f"  items: {len(payload)}")
        if payload:
            print(f"  last: {json.dumps(payload[-1], ensure_ascii=False)}")
    else:
        print(f"  data: {json.dumps(payload, ensure_ascii=False)}")
    return payload


def main():
    print("=== Cenhomes Location API Status ===")
    provinces_resp = _post("provinces", {})
    provinces = _print_result("provinces", provinces_resp) or []
    if not provinces:
        return

    first_province_id = provinces[-1].get("id")
    if not first_province_id:
        print("No province id found to test districts.")
        return
    time.sleep(0.2)

    districts_resp = _post("districts", {"limit": 100, "skip": 0, "provinceId": first_province_id})
    districts = _print_result(f"districts(province_id={first_province_id})", districts_resp) or []
    if not districts:
        return

    first_district_id = districts[-1].get("id")
    if not first_district_id:
        print("No district id found to test wards.")
        return
    time.sleep(0.2)

    wards_resp = _post("wards", {"limit": 100, "skip": 0, "districtIds": [first_district_id], "provinceId": first_province_id})
    _print_result(f"wards(district_id={first_district_id})", wards_resp)


if __name__ == "__main__":
    main()
