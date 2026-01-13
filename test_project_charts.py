import json
import requests

URL = "https://gateway.chotot.com/api-pty/public/market-price/charts?region=13000&category=1020&area=13107&ward=9231&include_current=true&bubble_ward_empty_street_price=true&disable_get_near_by=true&include_current=true&street_id=723e10b245416dfb23f48a16e4cf71df"
HEADERS = {
    "accept": "application/json; version=2",
    "origin": "https://www.nhatot.com",
    "referer": "https://www.nhatot.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "accept-encoding": "gzip, deflate, br, zstd",
}


def main():
    resp = requests.get(URL, headers=HEADERS, timeout=20)
    print("status:", resp.status_code)
    try:
        data = resp.json()
    except Exception:
        print(resp.text[:2000])
        return
    print(json.dumps(data, ensure_ascii=False, indent=2)[:4000])


if __name__ == "__main__":
    main()
