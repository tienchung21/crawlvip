#!/usr/bin/env python3
import argparse
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


DEFAULT_LIST_URL = "https://bds.com.vn/mua-ban-nha-dat-page2"
DEFAULT_DETAIL_URL = "https://bds.com.vn/can-ho-chung-cu-cao-cap-thuoc-du-an-imperia-sky-park-p808036.html"
LINK_SELECTOR = "a.image-item-nhadat[href]"


def build_page_url(base_list_url: str, page: int) -> str:
    if re.search(r"-page\d+/?$", base_list_url):
        return re.sub(r"-page\d+/?$", f"-page{page}", base_list_url)
    if base_list_url.endswith("/"):
        base_list_url = base_list_url[:-1]
    return f"{base_list_url}-page{page}"


def fetch_html(url: str) -> str:
    resp = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.text


def extract_detail_links(list_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    seen = set()
    for a in soup.select(LINK_SELECTOR):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        abs_url = urljoin(list_url, href)
        if not re.search(r"-p\d+\.html$", abs_url):
            continue
        if abs_url in seen:
            continue
        seen.add(abs_url)
        links.append(abs_url)
    return links


def main() -> int:
    parser = argparse.ArgumentParser(description="Test bds.com.vn listing manual paging")
    parser.add_argument("--list-url", default=DEFAULT_LIST_URL)
    parser.add_argument("--start-page", type=int, default=2)
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--detail-url", default=DEFAULT_DETAIL_URL)
    parser.add_argument("--show", type=int, default=5, help="Number of links to print per page")
    args = parser.parse_args()

    print(f"link_selector={LINK_SELECTOR}")
    print(f"detail_sample={args.detail_url}")
    print("")

    for page in range(args.start_page, args.start_page + args.pages):
        page_url = build_page_url(args.list_url, page)
        try:
            html = fetch_html(page_url)
            links = extract_detail_links(page_url, html)
            print(f"page={page}")
            print(f"url={page_url}")
            print(f"status=OK")
            print(f"links_found={len(links)}")
            for idx, link in enumerate(links[: args.show], start=1):
                print(f"{idx}. {link}")
            print("")
        except Exception as exc:
            print(f"page={page}")
            print(f"url={page_url}")
            print(f"status=ERROR")
            print(f"error={type(exc).__name__}: {exc}")
            print("")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
