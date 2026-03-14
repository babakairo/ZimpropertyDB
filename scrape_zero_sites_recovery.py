"""
Recover data from sites that returned zero in the latest run.

Currently handles:
  - newage_properties_zw (card data available on list page)
  - leengate_zw          (project cards + detail links)

Also probes but may report blocked/js-only:
  - propsearch
  - abcauctions
  - knight_frank_zw

Run:
  c:/Users/maung/Desktop/ZimProperties/.venv/Scripts/python.exe scrape_zero_sites_recovery.py
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from parsel import Selector

from scraper.utils.helpers import make_listing_id, parse_int, parse_price, parse_size, utc_now_iso

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

OUT_DIR = Path("scraper") / "data"
REPORT_DIR = Path("reports") / "selector_diagnostics"


def _fetch(url: str, timeout: int = 30) -> tuple[int | None, str, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, r.text, r.url
    except Exception as exc:
        return None, str(exc), url


def _base_item(source: str, listing_url: str, title: str) -> dict:
    return {
        "listing_id": make_listing_id(source, listing_url),
        "source": source,
        "property_title": title,
        "property_price": None,
        "currency": "USD",
        "property_type": None,
        "listing_type": "sale",
        "city": None,
        "suburb": None,
        "address_raw": None,
        "latitude": None,
        "longitude": None,
        "number_of_bedrooms": None,
        "number_of_bathrooms": None,
        "number_of_garages": None,
        "property_size_sqm": None,
        "property_size_raw": None,
        "stand_size_sqm": None,
        "features": [],
        "agent_name": None,
        "agent_phone": None,
        "agent_email": None,
        "agency_name": None,
        "image_urls": [],
        "listing_url": listing_url,
        "listing_date": None,
        "scraped_at": utc_now_iso(),
        "is_new_listing": True,
    }


def scrape_newage() -> tuple[list[dict], dict]:
    source = "newage_properties_zw"
    url = "https://www.newageproperties.co.zw/properties.php"
    status, html, final_url = _fetch(url)
    summary = {"site": source, "url": url, "final_url": final_url, "status": status, "records": 0}
    if status != 200:
        summary["note"] = "unreachable"
        return [], summary

    sel = Selector(text=html)
    cards = sel.css(".property-card")
    items: list[dict] = []

    for index, card in enumerate(cards, start=1):
        title = (card.css(".property-title::text").get() or "").strip()
        if not title:
            continue

        card_id = (card.css(".view-details-btn::attr(data-property-id)").get() or "").strip()
        listing_url = urljoin(final_url, f"properties.php#property-{card_id or index}")

        item = _base_item(source, listing_url, title)
        price_raw = (card.css(".property-price::text").get() or "").strip()
        item["property_price"], item["currency"] = parse_price(price_raw)

        item["property_type"] = (card.css(".property-badge::text").get() or "").strip().lower() or None
        status_text = (card.css(".property-status::text").get() or "").strip().lower()
        item["listing_type"] = "rent" if "rent" in status_text or "let" in status_text else "sale"

        location = (card.css(".property-location::text").get() or "").strip()
        item["city"] = location or None
        item["address_raw"] = location or title

        detail_spans = [s.strip() for s in card.css(".property-details span::text").getall() if s.strip()]
        beds_raw = next((s for s in detail_spans if "bed" in s.lower()), "")
        baths_raw = next((s for s in detail_spans if "bath" in s.lower()), "")
        size_raw = next((s for s in detail_spans if "sqm" in s.lower() or "m2" in s.lower() or "m²" in s.lower()), "")

        item["number_of_bedrooms"] = parse_int(beds_raw)
        item["number_of_bathrooms"] = parse_int(baths_raw)
        item["property_size_raw"] = size_raw or None
        item["property_size_sqm"] = parse_size(size_raw)

        image = (card.css("img::attr(data-src)").get() or card.css("img::attr(src)").get() or "").strip()
        if image:
            item["image_urls"] = [urljoin(final_url, image)]

        items.append(item)

    summary["records"] = len(items)
    return items, summary


def scrape_leengate() -> tuple[list[dict], dict]:
    source = "leengate_zw"
    url = "https://www.leengate.co.zw/available-stands/"
    status, html, final_url = _fetch(url)
    summary = {"site": source, "url": url, "final_url": final_url, "status": status, "records": 0}
    if status != 200:
        summary["note"] = "unreachable"
        return [], summary

    sel = Selector(text=html)
    links = [h for h in sel.css(".project h5 a::attr(href), .project a::attr(href)").getall() if h]
    links = list(dict.fromkeys(urljoin(final_url, h) for h in links))

    items: list[dict] = []
    for link in links:
        d_status, d_html, d_final = _fetch(link)
        if d_status != 200:
            continue
        d_sel = Selector(text=d_html)

        title = (d_sel.css("h1::text").get() or d_sel.css("title::text").get() or "").strip()
        if not title:
            continue

        item = _base_item(source, d_final, title)
        item["property_type"] = "land"
        item["listing_type"] = "sale"

        category = (d_sel.css(".elementor-post-info__terms-list-item::text").get() or "").strip()
        if category:
            item["features"] = [category]

        city_match = re.search(r"[-–]\s*([A-Za-z ]+)$", title)
        if city_match:
            item["city"] = city_match.group(1).strip()
        item["address_raw"] = title

        images = [urljoin(d_final, h) for h in d_sel.css("img::attr(src)").getall() if h]
        if images:
            item["image_urls"] = images[:8]

        item["agent_phone"] = (d_sel.css("a[href^='tel:']::attr(href)").get() or "").replace("tel:", "").strip() or None
        item["agency_name"] = "Leengate"

        items.append(item)

    summary["records"] = len(items)
    return items, summary


def probe_only(site: str, url: str) -> dict:
    status, html, final_url = _fetch(url)
    summary = {"site": site, "url": url, "final_url": final_url, "status": status, "records": 0}
    if status != 200:
        summary["note"] = "unreachable"
        return summary

    sel = Selector(text=html)
    anchors = len(sel.css("a[href]").getall())
    summary["anchors"] = anchors

    if anchors == 0:
        summary["note"] = "likely_js_or_blocked"
    else:
        summary["note"] = "reachable_but_no_parser_implemented"
    return summary


def write_jsonl(items: list[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def main() -> None:
    recovered: list[dict] = []
    summaries: list[dict] = []

    for fn in (scrape_newage, scrape_leengate):
        items, summary = fn()
        recovered.extend(items)
        summaries.append(summary)

    summaries.append(probe_only("propsearch", "https://www.propsearch.co.zw/for-sale/houses"))
    summaries.append(probe_only("abcauctions_co_zw", "https://www.abcauctions.co.zw/auctions/property"))
    summaries.append(probe_only("knight_frank_zw", "https://www.knightfrank.co.zw/properties-for-sale"))

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    out_jsonl = OUT_DIR / f"zero_sites_recovery_{ts}.jsonl"
    out_report = REPORT_DIR / f"zero_sites_recovery_report_{ts}.json"

    write_jsonl(recovered, out_jsonl)
    out_report.write_text(json.dumps({"summaries": summaries, "total_records": len(recovered)}, indent=2), encoding="utf-8")

    print(f"Recovered records: {len(recovered)}")
    print(f"Data file: {out_jsonl}")
    print(f"Report: {out_report}")
    print("Per-site summary:")
    for s in summaries:
        print(f"  {s['site']}: status={s.get('status')} records={s.get('records', 0)} note={s.get('note', '')}")


if __name__ == "__main__":
    main()
