#!/usr/bin/env python3
"""
Guland detail crawler (template-based).

Flow:
- Get PENDING links from collected_links (domain='guland.vn')
- Fetch detail HTML by curl_cffi
- Extract fields by template (CSS/XPath, valueType text/html/src/data-phone)
- Save to scraped_details, scraped_details_flat, scraped_detail_images
- Update link status to DONE / ERROR
"""

import argparse
import json
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import html as lxml_html

try:
    from curl_cffi import requests as cffi_requests
except Exception:
    cffi_requests = None

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    from database import Database


DOMAIN = "guland.vn"
DEFAULT_TEMPLATE = str(Path(__file__).resolve().parent / "template" / "gulanddetail.json")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
    "Referer": "https://guland.vn/",
}


def _is_xpath(selector: str) -> bool:
    sel = (selector or "").strip()
    return sel.startswith("/") or sel.startswith("(")


def _norm_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    t = re.sub(r"\s+", " ", str(s)).strip()
    return t or None


def _extract_style_url(style_text: str) -> List[str]:
    if not style_text:
        return []
    found = re.findall(r"url\((['\"]?)(.*?)\1\)", style_text)
    out = []
    for _, u in found:
        u = (u or "").strip()
        if u:
            out.append(u)
    return out


def _to_abs_url(url: str) -> str:
    return urljoin("https://guland.vn", (url or "").strip())


def _unique_keep_order(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _extract_src_values(node: Any, is_xpath_node: bool) -> List[str]:
    attrs = ["src", "data-src", "data-original", "data-lazy-src", "data-thumb", "data-image"]
    vals: List[str] = []

    def _get_attr(n: Any, key: str) -> Optional[str]:
        try:
            if is_xpath_node and hasattr(n, "get"):
                return n.get(key)
            if hasattr(n, "get"):
                return n.get(key)
        except Exception:
            return None
        return None

    for k in attrs:
        v = _get_attr(node, k)
        if v:
            vals.append(_to_abs_url(v))

    style_v = _get_attr(node, "style")
    if style_v:
        vals.extend([_to_abs_url(x) for x in _extract_style_url(style_v)])

    return _unique_keep_order([v for v in vals if v])


def _extract_data_phone(node: Any, is_xpath_node: bool) -> Optional[str]:
    attrs = ["data-phone", "data-mobile", "mobile", "data-phonenumber", "data-phone-number"]
    for k in attrs:
        try:
            v = node.get(k) if hasattr(node, "get") else None
        except Exception:
            v = None
        if v and str(v).strip():
            return _norm_text(v)

    # fallback to text
    if is_xpath_node:
        if isinstance(node, str):
            return _norm_text(node)
        return _norm_text(node.text_content() if hasattr(node, "text_content") else None)
    return _norm_text(node.get_text(" ", strip=True) if hasattr(node, "get_text") else None)


def _extract_field_value(field: Dict[str, Any], soup: BeautifulSoup, tree: Any) -> Any:
    selector = (field.get("selector") or "").strip()
    value_type = (field.get("valueType") or field.get("type") or "text").strip().lower()
    if not selector:
        return None

    is_xpath = _is_xpath(selector)
    try:
        nodes = tree.xpath(selector) if is_xpath else soup.select(selector)
    except Exception:
        nodes = []
    if not nodes:
        return None

    first = nodes[0]

    if value_type == "src":
        vals: List[str] = []
        for node in nodes:
            vals.extend(_extract_src_values(node, is_xpath))
        vals = _unique_keep_order(vals)
        return vals if vals else None

    if value_type == "href":
        href = first.get("href") if hasattr(first, "get") else None
        return _norm_text(_to_abs_url(href) if href else None)

    if value_type == "html":
        if is_xpath:
            try:
                return lxml_html.tostring(first, encoding="unicode").strip()
            except Exception:
                return None
        return str(first).strip()

    if value_type == "data-phone":
        return _extract_data_phone(first, is_xpath)

    if is_xpath:
        if isinstance(first, str):
            return _norm_text(first)
        return _norm_text(first.text_content() if hasattr(first, "text_content") else None)
    return _norm_text(first.get_text(" ", strip=True))


def extract_with_template(html_text: str, template: Dict[str, Any]) -> Dict[str, Any]:
    soup = BeautifulSoup(html_text, "html.parser")
    tree = lxml_html.fromstring(html_text)
    out: Dict[str, Any] = {}
    for field in template.get("fields", []):
        name = (field.get("name") or "").strip()
        if not name:
            continue
        out[name] = _extract_field_value(field, soup, tree)
    return out


def is_blocked(html_text: str) -> bool:
    t = (html_text or "").lower()
    # Avoid false-positive: normal pages may contain "cloudflare" script strings.
    blocked_markers = [
        "attention required!",
        "verify you are human",
        "please verify you are a human",
        "/cdn-cgi/challenge-platform/",
        "cf-chl-bypass",
        "g-recaptcha",
        "hcaptcha",
        "captcha",
    ]
    return any(m in t for m in blocked_markers)


def fetch_html(url: str) -> Optional[str]:
    if cffi_requests is None:
        raise RuntimeError("curl_cffi is required for guland_detail_crawler.py")
    try:
        resp = cffi_requests.get(
            url,
            headers=HEADERS,
            impersonate="chrome124",
            timeout=40,
        )
    except Exception as e:
        print(f"  [x] request failed: {e}")
        return None
    if resp.status_code != 200:
        print(f"  [x] HTTP {resp.status_code}")
        return None
    return resp.text or ""


def normalize_images(v: Any) -> List[str]:
    if isinstance(v, list):
        raw = v
    elif isinstance(v, str) and v.strip():
        raw = [v]
    else:
        raw = []
    out = []
    for x in raw:
        s = _norm_text(x)
        if not s:
            continue
        abs_url = _to_abs_url(s)
        if "map-icon.jpg" in abs_url:
            continue
        out.append(abs_url)
    return _unique_keep_order(out)


def run_full(
    template_path: str,
    batch_limit: int,
    delay_min: float,
    delay_max: float,
    max_consecutive_block: int,
) -> None:
    if not os.path.isfile(template_path):
        raise SystemExit(f"Template not found: {template_path}")
    with open(template_path, "r", encoding="utf-8") as f:
        template = json.load(f)

    db = Database()
    total_ok = 0
    total_err = 0
    cycle = 0
    consecutive_block = 0
    stopped_by_block = False

    while True:
        cycle += 1
        rows = db.get_pending_links(limit=batch_limit, domain=DOMAIN)
        print(f"[BATCH] cycle={cycle} pending_fetch={len(rows)}")
        if not rows:
            break

        for i, row in enumerate(rows, start=1):
            link_id = row["id"]
            url = row["url"]
            loaihinh = row.get("loaihinh")
            trade_type = row.get("trade_type")

            print(f"[{i}/{len(rows)}] Crawling id={link_id} url={url}")
            html_text = fetch_html(url)
            if not html_text or is_blocked(html_text):
                db.update_link_status(url, "ERROR")
                total_err += 1
                if html_text and is_blocked(html_text):
                    print("  -> blocked page, set ERROR")
                    consecutive_block += 1
                else:
                    print("  -> fetch failed, set ERROR")
                    consecutive_block = 0
                if consecutive_block > max_consecutive_block:
                    print(
                        f"[STOP] consecutive_block={consecutive_block} > "
                        f"max_consecutive_block={max_consecutive_block}"
                    )
                    stopped_by_block = True
                    break
                continue

            data = extract_with_template(html_text, template)
            images = normalize_images(data.get("img"))
            if images:
                data["img"] = images

            detail_id = db.add_scraped_detail_flat(
                url=url,
                data=data,
                domain=DOMAIN,
                link_id=link_id,
                loaihinh=loaihinh,
                trade_type=trade_type,
            )
            db.add_scraped_detail(
                url=url,
                data=data,
                domain=DOMAIN,
                link_id=link_id,
                success=bool(detail_id),
            )

            if detail_id:
                if images:
                    db.add_detail_images(detail_id=detail_id, images=images)
                db.update_link_status(url, "DONE")
                total_ok += 1
                consecutive_block = 0
                print(f"  -> Saved detail_id={detail_id}, images={len(images)}")
            else:
                db.update_link_status(url, "ERROR")
                total_err += 1
                consecutive_block = 0
                print("  -> save failed, set ERROR")

            delay_s = random.uniform(delay_min, delay_max)
            time.sleep(delay_s)
        if stopped_by_block:
            break

    print("=== DONE FULL GULAND DETAIL ===")
    print(
        f"saved_ok={total_ok} failed={total_err} "
        f"(stopped_by_block={stopped_by_block}, threshold=>{max_consecutive_block})"
    )


def run_single(url: str, template_path: str) -> None:
    if not os.path.isfile(template_path):
        raise SystemExit(f"Template not found: {template_path}")
    with open(template_path, "r", encoding="utf-8") as f:
        template = json.load(f)
    html_text = fetch_html(url)
    if not html_text:
        raise SystemExit("Cannot fetch URL")
    data = extract_with_template(html_text, template)
    imgs = normalize_images(data.get("img"))
    data["img"] = imgs if imgs else None
    print(json.dumps(data, ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="Guland detail crawler")
    parser.add_argument("--template", default=DEFAULT_TEMPLATE, help="Template JSON path")
    parser.add_argument("--full", action="store_true", help="Crawl all pending links for guland.vn")
    parser.add_argument("--url", default="", help="Test single URL and print JSON")
    parser.add_argument("--batch-limit", type=int, default=100, help="Batch size")
    parser.add_argument("--delay-min-seconds", type=float, default=0.5)
    parser.add_argument("--delay-max-seconds", type=float, default=1.5)
    parser.add_argument(
        "--max-consecutive-block",
        type=int,
        default=3,
        help="Stop full crawl when consecutive blocked pages > this value (default: 3)",
    )
    args = parser.parse_args()

    if args.url:
        run_single(url=args.url, template_path=args.template)
        return 0
    if args.full:
        run_full(
            template_path=args.template,
            batch_limit=args.batch_limit,
            delay_min=args.delay_min_seconds,
            delay_max=args.delay_max_seconds,
            max_consecutive_block=args.max_consecutive_block,
        )
        return 0
    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
