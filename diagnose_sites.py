"""
diagnose_sites.py

Config-aware selector diagnostics for Zimbabwe property spiders.

What it does:
  1) Loads spider configs (PropData, Zim Agent, Portal, Classifieds, ABC Auctions)
  2) Fetches one listing page per site
  3) Scores configured list selectors and proposes alternatives from live DOM
  4) Fetches one sample detail page and scores detail selectors per field
  5) Writes JSON reports to reports/selector_diagnostics/

Examples:
  python diagnose_sites.py
  python diagnose_sites.py --site propsearch
  python diagnose_sites.py --limit 5 --save-html
"""
import argparse
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from parsel import Selector

OUT_DIR = Path(__file__).parent / "reports" / "selector_diagnostics"
HTML_DIR = OUT_DIR / "html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

LISTING_URL_PATTERN = re.compile(
    r"/(?:listing|listings|property|properties|for-sale|to-rent|for-rent|to-let"
    r"|buy|rent|auction|lot|development|developments)/[a-z0-9][a-z0-9\-_/]{3,}",
    re.I,
)

PRICE_PATTERN = re.compile(r"(?:\$|usd|zwl|zar|gbp|eur|\d[\d,\. ]{2,})", re.I)
NUMBER_PATTERN = re.compile(r"\d+")


def _safe_import_configs() -> dict[str, Any]:
    try:
        from scraper.spiders.propdata_sites import SITES as PROPDATA_SITES
        from scraper.spiders.zimagents_spider import AGENCY_CONFIGS
        from scraper.spiders.portal_spiders import PORTALS
        from scraper.spiders.classifieds_co_zw import ClassifiedsCoZwSpider
        from scraper.spiders.abcauctions_co_zw import CATEGORY_PATHS
    except Exception as exc:
        sys.exit(
            "Failed to import spider configs. "
            "Ensure project dependencies are installed in this environment. "
            f"Details: {exc}"
        )

    targets: dict[str, dict[str, Any]] = {}

    for key, cfg in PROPDATA_SITES.items():
        targets[key] = {
            "group": "propdata",
            "base_url": cfg["base_url"],
            "list_paths": (cfg.get("sale_paths", []) + cfg.get("rent_paths", []))[:2],
            "configured_list_selectors": [
                'div[data-carousel="result"]::attr(data-href)',
                "a.gold-grid-thumbnails::attr(href)",
                "div.listing-result a::attr(href)",
            ],
            "configured_next_selectors": ["a[aria-label='Next']::attr(href)"],
            "configured_detail": {
                "title": ["h1#ListingTitle::text", "h1::text"],
                "price": ["div.price::text", "[class*='price']::text"],
                "beds": ["div.bed::text"],
                "baths": ["div.bath::text"],
                "size": ["div.area::text"],
                "images": ["img.swiper-lazy::attr(src)", "img.swiper-lazy::attr(data-src)"],
            },
        }

    for key, cfg in AGENCY_CONFIGS.items():
        targets[key] = {
            "group": "zim_agent",
            "base_url": cfg["base_url"],
            "list_paths": cfg.get("start_paths", [])[:2],
            "configured_list_selectors": [cfg.get("list", {}).get("links", "")],
            "configured_next_selectors": [cfg.get("list", {}).get("next", "")],
            "configured_detail": {
                field: [sel] for field, sel in cfg.get("detail", {}).items() if sel
            },
        }

    for key, cfg in PORTALS.items():
        targets[key] = {
            "group": "portal",
            "base_url": cfg["base_url"],
            "list_paths": (cfg.get("sale_paths", []) + cfg.get("rent_paths", []))[:2],
            "configured_list_selectors": list(cfg.get("list_links", [])),
            "configured_next_selectors": list(cfg.get("list_next", [])),
            "configured_detail": {
                field: list(selectors)
                for field, selectors in cfg.get("detail", {}).items()
            },
        }

    targets["classifieds_co_zw"] = {
        "group": "classifieds",
        "base_url": "https://www.classifieds.co.zw",
        "list_paths": [p for p, _, _ in ClassifiedsCoZwSpider.START_PATHS[:2]],
        "configured_list_selectors": [
            "div.listing-simple .title a::attr(href)",
            "div.listing-simple a[href*='/listings/']::attr(href)",
        ],
        "configured_next_selectors": [
            "link[rel='next']::attr(href)",
            "a[rel='next']::attr(href)",
        ],
        "configured_detail": {
            "title": ["h1.page-header::text", "h1::text"],
            "phone": ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
            "images": ["img::attr(src)", "img::attr(data-src)"],
        },
    }

    targets["abcauctions_co_zw"] = {
        "group": "abcauctions",
        "base_url": "https://www.abcauctions.co.zw",
        "list_paths": [path for path, _ in CATEGORY_PATHS[:2]],
        "configured_list_selectors": [
            "a.lot-card::attr(href)",
            "a.auction-lot::attr(href)",
            ".lot-listing a::attr(href)",
            "a[href*='/lot/']::attr(href)",
            "a[href*='/auction-lot/']::attr(href)",
            "a[href*='/property/']::attr(href)",
        ],
        "configured_next_selectors": [
            "a[rel='next']::attr(href)",
            "a[aria-label='Next']::attr(href)",
            "a.pagination-next::attr(href)",
        ],
        "configured_detail": {
            "title": ["h1.lot-title::text", "h1.auction-title::text", "h1::text"],
            "price": [".reserve-price::text", ".starting-bid::text", "[class*='price']::text"],
            "type": [".lot-category::text", ".property-type::text", ".category::text"],
            "images": ["img::attr(src)", "img::attr(data-src)"],
        },
    }

    return targets


def fetch(url: str, timeout: int) -> tuple[int | None, str, str]:
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return response.status_code, response.text, response.url
    except Exception as exc:
        return None, str(exc), url


def is_listing_like(href: str) -> bool:
    href_lower = (href or "").lower()
    if href_lower.startswith(("javascript:", "mailto:", "tel:")):
        return False
    return bool(LISTING_URL_PATTERN.search(href_lower))


def _extract_values(sel: Selector, css: str) -> list[str]:
    if not css:
        return []
    try:
        values = sel.css(css).getall()
        cleaned = [v.strip() for v in values if isinstance(v, str) and v and v.strip()]
        return cleaned
    except Exception:
        return []


def _score_link_selector(sel: Selector, css: str, base_url: str) -> dict[str, Any]:
    values = _extract_values(sel, css)
    abs_links = []
    for href in values:
        abs_url = urljoin(base_url, href)
        if is_listing_like(abs_url):
            abs_links.append(abs_url)
    unique_links = list(dict.fromkeys(abs_links))
    return {
        "selector": css,
        "total_matches": len(values),
        "listing_like_matches": len(unique_links),
        "sample": unique_links[:5],
    }


def _auto_discover_link_selectors(sel: Selector, base_url: str) -> list[str]:
    selector_candidates = set()
    hrefs = _extract_values(sel, "a::attr(href)")
    listing_hrefs = [h for h in hrefs if is_listing_like(urljoin(base_url, h))]

    for href in listing_hrefs:
        path = urlparse(urljoin(base_url, href)).path
        segments = [s for s in path.split("/") if s]
        if segments:
            selector_candidates.add(f"a[href*='/{segments[0]}/']::attr(href)")
        if len(segments) > 1:
            selector_candidates.add(f"a[href*='/{segments[0]}/{segments[1]}']::attr(href)")

    for cls in _extract_values(sel, "a[class]::attr(class)")[:120]:
        for token in cls.split():
            token = token.strip()
            if token and len(token) > 2:
                selector_candidates.add(f"a.{token}::attr(href)")

    generic = {
        "a[href*='/listing/']::attr(href)",
        "a[href*='/listings/']::attr(href)",
        "a[href*='/property/']::attr(href)",
        "a[href*='/properties/']::attr(href)",
        "a[href*='/for-sale/']::attr(href)",
        "a[href*='/to-rent/']::attr(href)",
        "a[href*='/auction/']::attr(href)",
        "a[href*='/lot/']::attr(href)",
    }
    selector_candidates.update(generic)
    return sorted(selector_candidates)


def _field_default_candidates(field: str) -> list[str]:
    defaults = {
        "title": ["h1::text", "title::text", "[class*='title']::text"],
        "price": ["[class*='price']::text", "[id*='price']::text"],
        "type": ["[class*='type']::text", "[class*='category']::text"],
        "suburb": ["[class*='suburb']::text", "[class*='location']::text"],
        "city": ["[class*='city']::text", "[class*='region']::text"],
        "beds": ["[class*='bed']::text", "[data-beds]::attr(data-beds)"],
        "baths": ["[class*='bath']::text", "[data-baths]::attr(data-baths)"],
        "size": ["[class*='size']::text", "[class*='area']::text"],
        "agent": ["[class*='agent']::text", "[class*='contact']::text"],
        "phone": ["a[href^='tel:']::attr(href)", "a[href^='tel:']::text"],
        "images": ["img::attr(src)", "img::attr(data-src)"],
        "date": ["time::attr(datetime)", "[class*='date']::text"],
    }
    return defaults.get(field, [f"[class*='{field}']::text"])


def _score_field_selector(field: str, sel: Selector, css: str) -> dict[str, Any]:
    values = _extract_values(sel, css)
    if field == "price":
        values = [v for v in values if PRICE_PATTERN.search(v)]
    elif field in {"beds", "baths", "size"}:
        values = [v for v in values if NUMBER_PATTERN.search(v)]
    elif field == "images":
        values = [v for v in values if v.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".avif")) or "/image" in v.lower()]

    return {
        "selector": css,
        "match_count": len(values),
        "sample": values[:3],
    }


def diagnose_site(name: str, cfg: dict[str, Any], timeout: int, save_html: bool) -> dict[str, Any]:
    result: dict[str, Any] = {
        "site": name,
        "group": cfg["group"],
        "base_url": cfg["base_url"],
        "list_page": None,
        "list_selector_scores": [],
        "list_selector_suggestions": [],
        "sample_listing_url": None,
        "detail_page": None,
        "detail_field_scores": {},
        "errors": [],
    }

    list_path = (cfg.get("list_paths") or ["/"])[0] or "/"
    list_url = cfg["base_url"] + list_path
    status, html, final_url = fetch(list_url, timeout=timeout)

    if status is None:
        result["errors"].append(f"list fetch failed: {html}")
        return result

    result["list_page"] = {
        "requested": list_url,
        "final": final_url,
        "status": status,
    }
    if status != 200:
        result["errors"].append(f"list status={status}")
        return result

    if save_html:
        HTML_DIR.mkdir(parents=True, exist_ok=True)
        (HTML_DIR / f"{name}_list.html").write_text(html, encoding="utf-8", errors="replace")

    list_sel = Selector(text=html)

    configured_selectors = [s for s in cfg.get("configured_list_selectors", []) if s]
    for css in configured_selectors:
        result["list_selector_scores"].append(_score_link_selector(list_sel, css, cfg["base_url"]))

    auto_candidates = _auto_discover_link_selectors(list_sel, cfg["base_url"])
    auto_scores = [_score_link_selector(list_sel, css, cfg["base_url"]) for css in auto_candidates]
    auto_scores = [s for s in auto_scores if s["listing_like_matches"] > 0]
    auto_scores.sort(key=lambda x: (x["listing_like_matches"], x["total_matches"]), reverse=True)
    result["list_selector_suggestions"] = auto_scores[:8]

    all_ranked = sorted(
        result["list_selector_scores"] + result["list_selector_suggestions"],
        key=lambda x: (x["listing_like_matches"], x["total_matches"]),
        reverse=True,
    )
    if all_ranked and all_ranked[0]["sample"]:
        result["sample_listing_url"] = all_ranked[0]["sample"][0]

    if not result["sample_listing_url"]:
        return result

    detail_status, detail_html, detail_final = fetch(result["sample_listing_url"], timeout=timeout)
    result["detail_page"] = {
        "requested": result["sample_listing_url"],
        "final": detail_final,
        "status": detail_status,
    }
    if detail_status != 200:
        result["errors"].append(f"detail status={detail_status}")
        return result

    if save_html:
        (HTML_DIR / f"{name}_detail.html").write_text(detail_html, encoding="utf-8", errors="replace")

    detail_sel = Selector(text=detail_html)
    for field, selectors in cfg.get("configured_detail", {}).items():
        pool = list(dict.fromkeys([s for s in selectors if s] + _field_default_candidates(field)))
        field_scores = [_score_field_selector(field, detail_sel, css) for css in pool]
        field_scores = [s for s in field_scores if s["match_count"] > 0]
        field_scores.sort(key=lambda x: x["match_count"], reverse=True)
        result["detail_field_scores"][field] = field_scores[:5]

    return result


def print_summary(results: list[dict[str, Any]]) -> None:
    print("\nSelector diagnostics summary")
    print("=" * 90)
    for row in results:
        list_page = row.get("list_page") or {}
        detail_page = row.get("detail_page") or {}
        list_status = list_page.get("status")
        detail_status = detail_page.get("status")
        top_list = (row.get("list_selector_suggestions") or row.get("list_selector_scores") or [{}])[0]
        top_list_selector = top_list.get("selector", "-")
        top_list_hits = top_list.get("listing_like_matches", 0)
        print(
            f"{row['site']:<24} "
            f"list={str(list_status):<4} "
            f"detail={str(detail_status):<4} "
            f"links={top_list_hits:<3} "
            f"best={top_list_selector}"
        )
        if row["errors"]:
            print(f"  errors: {' | '.join(row['errors'])}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", help="Exact site key filter (e.g. propsearch)")
    parser.add_argument("--limit", type=int, default=0, help="Max number of sites")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--save-html", action="store_true")
    args = parser.parse_args()

    targets = _safe_import_configs()
    if args.site:
        if args.site not in targets:
            available = ", ".join(sorted(targets.keys()))
            sys.exit(f"Unknown site '{args.site}'. Available: {available}")
        targets = {args.site: targets[args.site]}

    target_items = list(targets.items())
    if args.limit > 0:
        target_items = target_items[:args.limit]

    print(f"Loaded {len(target_items)} targets")
    results = []

    for index, (site, cfg) in enumerate(target_items, start=1):
        print(f"\n[{index}/{len(target_items)}] {site} ({cfg['group']})")
        result = diagnose_site(site, cfg, timeout=args.timeout, save_html=args.save_html)
        results.append(result)
        time.sleep(0.8)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = OUT_DIR / f"selector_diagnostics_{ts}.json"
    output_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    grouped: dict[str, int] = defaultdict(int)
    for r in results:
        if r.get("sample_listing_url"):
            grouped[r["group"]] += 1

    print_summary(results)
    print("\nSaved report:")
    print(output_path)
    print("\nSites with at least one detected listing URL by group:")
    for group, count in sorted(grouped.items()):
        print(f"  {group}: {count}")


if __name__ == "__main__":
    main()
