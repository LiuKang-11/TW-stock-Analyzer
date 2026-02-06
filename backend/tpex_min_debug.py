#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
from urllib.parse import urljoin, urlparse

import requests


UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

# 你可以把這些頁面當「入口頁」：先找到真正的資料 API / 下載連結
ENTRY_PAGES = {
    # 個股日成交資訊（你現在要先驗證的股價）
    "stock_pricing_zh": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/stock-pricing.html",
    "stock_pricing_en": "https://www.tpex.org.tw/en-us/mainboard/trading/info/stock-pricing.html",

    # 外資/三大法人（先放著，等你要挖 foreign-url 再用）
    "inst_summary_en": "https://www.tpex.org.tw/en-us/mainboard/trading/major-institutional/summary/day.html",
    "inst_detail_en":  "https://www.tpex.org.tw/en-us/mainboard/trading/major-institutional/detail/day.html",

    # 融資融券（先放著，等你要挖 margin-url 再用）
    "margin_tx_zh": "https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/transactions.html",
}


def _safe_decode(resp: requests.Response) -> str:
    """
    避免亂碼：requests 可能用 ISO-8859-1 猜，改用 apparent_encoding fallback。
    """
    # 若 server 有給 encoding 就尊重；否則用 apparent_encoding
    if not resp.encoding or resp.encoding.lower() in ("iso-8859-1", "latin-1"):
        resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def fetch(session: requests.Session, url: str, referer: str | None = None) -> tuple[int, str, str]:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }
    if referer:
        headers["Referer"] = referer

    r = session.get(url, headers=headers, timeout=30, allow_redirects=True)
    text = _safe_decode(r)
    final_url = r.url
    return r.status_code, final_url, text


def extract_candidate_urls(base_url: str, html: str) -> list[str]:
    """
    從 HTML/JS 內容抓出可能的資料端點（相對/絕對 URL 皆可）。
    你後續可以把這些 candidate 逐一 request 測試 response type (json/csv/html)。
    """
    candidates: set[str] = set()

    # 常見：fetch("..."), axios.get("..."), $.ajax({url:"..."})
    patterns = [
        r"""fetch\(\s*["']([^"']+)["']""",
        r"""axios\.(?:get|post)\(\s*["']([^"']+)["']""",
        r"""\$.ajax\(\s*\{[^}]*url\s*:\s*["']([^"']+)["']""",
        r"""url\s*:\s*["']([^"']+)["']""",
        r"""href\s*=\s*["']([^"']+)["']""",
        r"""action\s*=\s*["']([^"']+)["']""",
    ]

    for pat in patterns:
        for m in re.finditer(pat, html, flags=re.IGNORECASE):
            u = m.group(1).strip()
            if not u:
                continue
            # 排除純錨點、javascript:
            if u.startswith("#") or u.lower().startswith("javascript:"):
                continue
            # 統一成絕對 URL
            abs_u = urljoin(base_url, u)
            candidates.add(abs_u)

    # 再加一層：直接掃描看起來像 API / CSV / JSON / PHP 的字串
    extra = re.findall(
        r"""(https?://[^\s"']+|/[A-Za-z0-9_\-./]+(?:\?(?:[^\s"'<>])*)?)""",
        html
    )
    for u in extra:
        if any(k in u.lower() for k in ("api", "json", "csv", ".php", "download")):
            candidates.add(urljoin(base_url, u))

    # 過濾：只保留同網域或可信的絕對路徑（你也可放寬）
    base_host = urlparse(base_url).netloc
    filtered = []
    for u in sorted(candidates):
        host = urlparse(u).netloc
        if host and host != base_host:
            continue
        filtered.append(u)
    return filtered


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", default="8390", help="stock code for display only (not used to call API yet)")
    ap.add_argument("--show-preview", action="store_true", help="print html preview")
    ap.add_argument("--max-links", type=int, default=80, help="max candidate urls to print per page")
    args = ap.parse_args()

    with requests.Session() as s:
        for name, url in ENTRY_PAGES.items():
            print("\n" + "=" * 110)
            print(f"[ENTRY] {name}")
            st, final_url, html = fetch(s, url, referer="https://www.tpex.org.tw/")
            print(f"[HTTP] {st}  final={final_url}")

            if args.show_preview:
                preview = html[:800].replace("\n", "\\n")
                print(f"[HTML preview 800 chars]\n{preview}\n")

            # 抓候選端點
            links = extract_candidate_urls(final_url, html)

            print(f"[CANDIDATES] found={len(links)} (show up to {args.max_links})")
            for i, u in enumerate(links[: args.max_links], 1):
                print(f"  {i:02d}. {u}")

            # 你可在這裡針對 links 做進一步自動測試：
            # - 若 url 含 csv/json/php/api 等字樣，就 GET 一次看看 content-type 與 body 開頭
            # 我先留你一個提示，不直接打爆 TPEx：
            testable = [u for u in links if any(k in u.lower() for k in ("csv", "json", "api", ".php"))]
            print(f"[HINT] testable-like endpoints: {len(testable)} (csv/json/api/php)")


if __name__ == "__main__":
    main()
