"""
reports/report_builder.py
Generates a multi-page property intelligence PDF report using ReportLab.

Usage:
    from reports.report_builder import generate_report
    path = generate_report(request_dict, "/path/to/output.pdf")
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from analytics.suburb_queries import (
    get_suburb_snapshot,
    get_price_trend,
    get_comparable_listings,
    get_active_agents,
)
from analytics.chart_generator import generate_price_trend_chart

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER
from reportlab.platypus import (
    BaseDocTemplate, PageTemplate, Frame,
    Paragraph, Spacer, Table, TableStyle, Image,
    HRFlowable, KeepTogether,
)
from reportlab.platypus.flowables import Flowable

# ── Colour palette ────────────────────────────────────────────────────────────
NAVY       = colors.HexColor("#0D1B3E")
TEAL       = colors.HexColor("#00BCD4")
ORANGE     = colors.HexColor("#FF9800")
LIGHT_NAVY = colors.HexColor("#F0F4F8")
GREY_MID   = colors.HexColor("#888888")
GREY_LIGHT = colors.HexColor("#F5F5F5")
GREEN      = colors.HexColor("#2E7D32")
WHITE      = colors.white

PAGE_W, PAGE_H = A4
MARGIN        = 0.65 * inch
HEADER_HEIGHT = 0.6 * inch
CONTENT_W     = PAGE_W - 2 * MARGIN

# ── Page header / footer ──────────────────────────────────────────────────────

def _make_page_callback(suburb: str, listing_type: str):
    label = "FOR SALE" if listing_type == "sale" else "FOR RENT"

    def on_page(canvas, doc):
        canvas.saveState()
        w = PAGE_W

        # Navy header
        canvas.setFillColor(NAVY)
        canvas.rect(0, PAGE_H - HEADER_HEIGHT, w, HEADER_HEIGHT, fill=1, stroke=0)

        # Left: Vamba Data
        canvas.setFillColor(WHITE)
        canvas.setFont("Helvetica-Bold", 14)
        canvas.drawString(MARGIN, PAGE_H - HEADER_HEIGHT + 0.18 * inch, "Vamba Data")

        # Right: report title
        canvas.setFont("Helvetica", 10)
        canvas.drawRightString(
            w - MARGIN,
            PAGE_H - HEADER_HEIGHT + 0.18 * inch,
            f"Property Intelligence Report — {suburb}",
        )

        # Teal rule
        canvas.setStrokeColor(TEAL)
        canvas.setLineWidth(1)
        canvas.line(0, PAGE_H - HEADER_HEIGHT, w, PAGE_H - HEADER_HEIGHT)

        # Footer: page number
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(GREY_MID)
        canvas.drawRightString(w - MARGIN, 0.35 * inch, f"Page {doc.page}")

        canvas.restoreState()

    return on_page


# ── Section heading with teal left border ─────────────────────────────────────

class SectionHeading(Flowable):
    def __init__(self, text: str, width: float):
        super().__init__()
        self._text  = text
        self._width = width
        self._height = 0.28 * inch

    def wrap(self, avail_w, avail_h):
        return self._width, self._height

    def draw(self):
        c = self.canv
        h = self._height
        c.setFillColor(TEAL)
        c.rect(0, 0, 3, h, fill=1, stroke=0)
        c.setFillColor(NAVY)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(10, h * 0.3, self._text)


# ── Listing-type badge (FOR SALE / FOR RENT) ──────────────────────────────────

class ListingTypeBadge(Flowable):
    """Large coloured badge displayed immediately below the suburb name."""

    def __init__(self, listing_type: str, width: float):
        super().__init__()
        self._lt    = listing_type
        self._label = "FOR SALE PROPERTIES" if listing_type == "sale" else "RENTAL PROPERTIES"
        self._color = TEAL if listing_type == "sale" else ORANGE
        self._width = width
        self._height = 0.36 * inch

    def wrap(self, avail_w, avail_h):
        return self._width, self._height

    def draw(self):
        c = self.canv
        badge_w = 2.8 * inch
        badge_h = self._height
        # Filled pill
        c.setFillColor(self._color)
        c.roundRect(0, 0, badge_w, badge_h, radius=4, fill=1, stroke=0)
        # Label
        c.setFillColor(WHITE)
        c.setFont("Helvetica-Bold", 13)
        c.drawCentredString(badge_w / 2, badge_h * 0.3, self._label)


# ── Styles ────────────────────────────────────────────────────────────────────

def _styles():
    s = {}
    s["title_main"] = ParagraphStyle(
        "title_main", fontName="Helvetica-Bold",
        fontSize=16, textColor=NAVY, spaceAfter=2,
    )
    s["title_suburb"] = ParagraphStyle(
        "title_suburb", fontName="Helvetica-Bold",
        fontSize=28, textColor=NAVY, spaceAfter=6,
    )
    s["generated"] = ParagraphStyle(
        "generated", fontName="Helvetica",
        fontSize=9, textColor=GREY_MID, spaceAfter=6,
    )
    s["stat_label"] = ParagraphStyle(
        "stat_label", fontName="Helvetica",
        fontSize=8, textColor=GREY_MID, spaceBefore=0, spaceAfter=1,
    )
    s["stat_value"] = ParagraphStyle(
        "stat_value", fontName="Helvetica-Bold",
        fontSize=16, textColor=NAVY, spaceBefore=0, spaceAfter=4,
    )
    s["note_grey"] = ParagraphStyle(
        "note_grey", fontName="Helvetica-Oblique",
        fontSize=8, textColor=GREY_MID, spaceAfter=4,
    )
    s["note_orange"] = ParagraphStyle(
        "note_orange", fontName="Helvetica-Oblique",
        fontSize=8, textColor=ORANGE, spaceAfter=4,
    )
    s["bullet"] = ParagraphStyle(
        "bullet", fontName="Helvetica",
        fontSize=10, textColor=NAVY, leading=15,
        leftIndent=10, spaceAfter=6,
    )
    s["table_header"] = ParagraphStyle(
        "table_header", fontName="Helvetica-Bold",
        fontSize=8, textColor=WHITE,
    )
    s["table_cell"] = ParagraphStyle(
        "table_cell", fontName="Helvetica",
        fontSize=8, textColor=NAVY,
    )
    s["table_cell_orange"] = ParagraphStyle(
        "table_cell_orange", fontName="Helvetica",
        fontSize=8, textColor=ORANGE,
    )
    s["disclaimer"] = ParagraphStyle(
        "disclaimer", fontName="Helvetica-Oblique",
        fontSize=7, textColor=GREY_MID, leading=10, spaceAfter=4,
    )
    s["centre_grey"] = ParagraphStyle(
        "centre_grey", fontName="Helvetica",
        fontSize=8, textColor=GREY_MID, alignment=TA_CENTER,
    )
    s["grey_box"] = ParagraphStyle(
        "grey_box", fontName="Helvetica-Oblique",
        fontSize=9, textColor=GREY_MID, alignment=TA_CENTER,
        spaceAfter=4,
    )
    return s


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_price(val) -> str:
    if val is None:
        return "N/A"
    try:
        return f"${float(val):,.0f}"
    except (ValueError, TypeError):
        return "N/A"


def _fmt_dom(val, cap: int = 999) -> str:
    """Format days-on-market with a display cap."""
    if val is None:
        return "N/A"
    try:
        d = int(val)
        if d > cap:
            return f"{cap}+ days"
        return f"{d} days"
    except (ValueError, TypeError):
        return "N/A"


def _trend_interpretation(trend: list, snap: dict) -> str:
    if len(trend) < 4:
        return ""
    prices = [t["median_price"] for t in trend if t["median_price"] is not None]
    if len(prices) < 4:
        return ""
    half = len(prices) // 2
    first_h  = sum(prices[:half]) / half
    second_h = sum(prices[half:]) / (len(prices) - half)
    pct = (second_h - first_h) / first_h * 100 if first_h else 0
    suburb = snap["suburb_name"]
    if abs(pct) < 2:
        return (f"Over the past 12 weeks, median asking prices in {suburb} have "
                f"remained stable, hovering around {_fmt_price(prices[-1])}.")
    elif pct > 0:
        return (f"Over the past 12 weeks, median asking prices in {suburb} have "
                f"increased by {abs(pct):.1f}%, rising from {_fmt_price(prices[0])} "
                f"to {_fmt_price(prices[-1])}.")
    else:
        return (f"Over the past 12 weeks, median asking prices in {suburb} have "
                f"declined by {abs(pct):.1f}%, falling from {_fmt_price(prices[0])} "
                f"to {_fmt_price(prices[-1])}.")


def _advisory_bullets(snap: dict, trend: list, comps: list) -> list[str]:
    bullets = []
    suburb = snap["suburb_name"]
    lt     = snap.get("listing_type", "sale")
    count  = snap.get("active_listing_count", 0)
    temp   = snap.get("market_temperature", "")

    # Bullet 1: market temperature
    if temp == "Buyer's Market":
        bullets.append(
            f"With {count} active {'for-sale' if lt=='sale' else 'rental'} listings, "
            f"{suburb} is currently a <b>Buyer's Market</b>. Buyers have negotiating "
            f"power — consider offering below asking price."
        )
    elif temp == "Seller's Market":
        bullets.append(
            f"Only {count} {'for-sale' if lt=='sale' else 'rental'} listings are "
            f"active in {suburb}, indicating a <b>Seller's Market</b>. "
            f"Act quickly and submit competitive offers."
        )
    else:
        bullets.append(
            f"{suburb} has {count} active listings — a <b>Balanced Market</b> "
            f"where neither buyers nor sellers hold a significant advantage."
        )

    # Bullet 2: price insight
    median = snap.get("median_asking_price")
    avg    = snap.get("avg_asking_price")
    if median and avg and avg > 0:
        skew = (avg - median) / avg * 100
        if skew > 20:
            bullets.append(
                f"The average asking price ({_fmt_price(avg)}) is significantly "
                f"higher than the median ({_fmt_price(median)}), suggesting a small "
                f"number of premium properties are pulling the average up. "
                f"The median is a better benchmark for typical buyers."
            )
        else:
            bullets.append(
                f"The median asking price in {suburb} is {_fmt_price(median)}, "
                f"with an average of {_fmt_price(avg)}. Price distribution is "
                f"relatively consistent across available stock."
            )

    # Bullet 3: data availability note for days on market
    scrape_date = snap.get("data_freshness") or date.today().strftime("%d %B %Y")
    bullets.append(
        f"Days on market data is available for listings sourced from property.co.zw. "
        f"Listings from other portals show N/A — these were verified current as of {scrape_date}."
    )

    # Bullet 4: zombie / old listings warning
    over_1yr = snap.get("over_one_year_count", 0)
    if over_1yr > 0:
        bullets.append(
            f"<b>{over_1yr} listing{'s' if over_1yr > 1 else ''}</b> in {suburb} "
            f"{'have' if over_1yr > 1 else 'has'} been on the market for over 1 year "
            f"and may be outdated. These could be incorrectly priced, already "
            f"transacted, or test listings. Focus on listings under 90 days for the "
            f"most accurate market picture."
        )
    else:
        stale_count = sum(1 for c in comps if c.get("is_stale"))
        if stale_count > 0:
            bullets.append(
                f"{stale_count} of the comparable listings shown have been on the "
                f"market for 90+ days. Sellers of these properties are statistically "
                f"more likely to accept offers below the asking price."
            )

    # Bullet 5: transaction price disclaimer (always last)
    bullets.append(
        "<b>Important:</b> All prices shown are <i>asking prices</i> only. "
        "Final transaction prices are not publicly disclosed in Zimbabwe. "
        "Actual sale prices may differ significantly from listed prices."
    )

    return bullets[:5]


# ── Source count helper ───────────────────────────────────────────────────────

def _get_source_count(suburb: str, listing_type: str) -> int:
    """Returns the number of distinct scraper sources contributing listings
    to this suburb for the given listing_type. Excludes wayback sources."""
    from analytics.suburb_queries import _get_conn
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(DISTINCT ds.source)
            FROM ZIM_PROPERTY_DB.WAREHOUSE.FACT_LISTINGS fl
            JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_LOCATION dl
                ON dl.location_key = fl.location_key
            JOIN ZIM_PROPERTY_DB.WAREHOUSE.DIM_SOURCE ds
                ON ds.source_key = fl.source_key
            WHERE LOWER(dl.suburb_clean) = LOWER(%s)
              AND fl.listing_type = %s
              AND fl.is_price_valid = TRUE
              AND fl.property_price_usd > 0
              AND ds.source NOT LIKE '%%archive.org%%'
              AND ds.source NOT LIKE '%%wayback%%'
            """,
            (suburb, listing_type),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] else 0
    finally:
        conn.close()


# ── Stat grid ─────────────────────────────────────────────────────────────────

def _stat_grid(snap: dict, st: dict, source_count: int = 0) -> Table:
    stats = [
        ("ACTIVE LISTINGS",      str(snap.get("active_listing_count", "N/A"))),
        ("MEDIAN ASKING PRICE",  _fmt_price(snap.get("median_asking_price"))),
        ("AVERAGE ASKING PRICE", _fmt_price(snap.get("avg_asking_price"))),
        ("PRICE RANGE",          f"{_fmt_price(snap.get('min_asking_price'))} — "
                                 f"{_fmt_price(snap.get('max_asking_price'))}"),
        ("SOURCES",              f"{source_count} portal{'s' if source_count != 1 else ''}"),
        ("MARKET CONDITIONS",    snap.get("market_temperature", "N/A")),
    ]

    col_w = CONTENT_W / 2
    rows  = []
    for i in range(0, len(stats), 2):
        row = []
        for label, value in stats[i:i+2]:
            row.append([
                Paragraph(label, st["stat_label"]),
                Paragraph(value, st["stat_value"]),
            ])
        if len(row) == 1:
            row.append("")
        rows.append(row)

    tbl = Table(rows, colWidths=[col_w, col_w])
    tbl.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    return tbl


# ── Comparable listings table ─────────────────────────────────────────────────

def _comps_table(comps: list, st: dict) -> Table:
    headers    = ["Property Type", "Beds", "Asking Price", "On Market", "Agent", "Source"]
    col_widths = [
        CONTENT_W * 0.16,
        CONTENT_W * 0.07,
        CONTENT_W * 0.16,
        CONTENT_W * 0.13,
        CONTENT_W * 0.24,
        CONTENT_W * 0.24,
    ]

    data = [[Paragraph(h, st["table_header"]) for h in headers]]

    stale_rows = []
    for i, c in enumerate(comps):
        row_idx = i + 1
        ptype   = (c.get("property_type") or "Unknown").title()
        beds    = str(c.get("bedrooms")) if c.get("bedrooms") is not None else "—"
        price   = _fmt_price(c.get("asking_price"))
        dom_raw = c.get("days_on_market")
        dom     = _fmt_dom(dom_raw, cap=999)
        agent   = (c.get("agent_name") or "—")[:40]
        url     = c.get("source_url") or "—"
        if len(url) > 40:
            url = url[:37] + "..."

        if c.get("is_stale"):
            dom = dom + " *"
            stale_rows.append(row_idx)
        if c.get("price_reduction_detected"):
            price = price + " \u2193"

        # Use orange text for stale rows
        cell_style = st["table_cell_orange"] if c.get("is_stale") else st["table_cell"]
        data.append([
            Paragraph(ptype,  cell_style),
            Paragraph(beds,   cell_style),
            Paragraph(price,  cell_style),
            Paragraph(dom,    cell_style),
            Paragraph(agent,  cell_style),
            Paragraph(url,    cell_style),
        ])

    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    style_cmds = [
        ("BACKGROUND",    (0, 0), (-1, 0), NAVY),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0), 8),
        ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",      (0, 1), (-1, -1), 8),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("GRID",          (0, 0), (-1, -1), 0.25, colors.HexColor("#DDDDDD")),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]
    for ri in range(1, len(data)):
        bg = GREY_LIGHT if ri % 2 == 0 else WHITE
        style_cmds.append(("BACKGROUND", (0, ri), (-1, ri), bg))

    tbl.setStyle(TableStyle(style_cmds))
    return tbl


# ── Agent table ───────────────────────────────────────────────────────────────

def _agents_table(agents: list, st: dict) -> Table:
    headers    = ["Agent / Agency", "Active Listings", "Phone", "Email"]
    col_widths = [CONTENT_W * 0.30, CONTENT_W * 0.15,
                  CONTENT_W * 0.22, CONTENT_W * 0.33]

    data = [[Paragraph(h, st["table_header"]) for h in headers]]
    for a in agents:
        name_agency = f"{a.get('agent_name', '')} / {a.get('agency_name') or '—'}"
        phone = a.get("agent_phone") or "—"
        email = a.get("agent_email") or "—"
        if phone == "—" and email == "—":
            phone = "Contact via listing portal"
            email = ""
        data.append([
            Paragraph(name_agency[:55], st["table_cell"]),
            Paragraph(str(a.get("active_listing_count", 0)), st["table_cell"]),
            Paragraph(phone, st["table_cell"]),
            Paragraph(email[:45], st["table_cell"]),
        ])

    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), NAVY),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0), 8),
        ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",      (0, 1), (-1, -1), 8),
        ("TEXTCOLOR",     (0, 1), (-1, -1), NAVY),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("GRID",          (0, 0), (-1, -1), 0.25, colors.HexColor("#DDDDDD")),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return tbl


# ── Main PDF generator ────────────────────────────────────────────────────────

def generate_report(request: dict, output_path: str) -> str:
    """
    Generates a complete property intelligence PDF report.

    Parameters
    ----------
    request : dict
        Keys: suburb_name, listing_type (default 'sale'), property_type (opt),
        bedrooms (opt), budget_min (opt), budget_max (opt),
        buyer_email, report_id
    output_path : str
        Destination path for the PDF.

    Returns
    -------
    str
        output_path on success.
    """
    suburb      = request["suburb_name"]
    listing_type = request.get("listing_type", "sale")
    ptype       = request.get("property_type")
    bedrooms    = request.get("bedrooms")
    budget_min  = request.get("budget_min")
    budget_max  = request.get("budget_max")
    report_id   = request.get("report_id", "REPORT")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # ── Fetch data ──────────────────────────────────────────────────────────
    snap   = get_suburb_snapshot(suburb, ptype, bedrooms, budget_min, budget_max,
                                 listing_type=listing_type)
    trend  = get_price_trend(suburb, weeks=12, property_type=ptype,
                             listing_type=listing_type)
    comps  = get_comparable_listings(suburb, ptype, bedrooms, budget_min, budget_max,
                                     limit=8, listing_type=listing_type)
    agents = get_active_agents(suburb, limit=5)

    fewer_than_3 = len(comps) < 3
    if fewer_than_3:
        comps = get_comparable_listings(suburb, limit=8, listing_type=listing_type)

    # ── Trend chart ─────────────────────────────────────────────────────────
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        chart_path = tmp.name
    generate_price_trend_chart(trend, suburb, chart_path)

    # ── Document setup ──────────────────────────────────────────────────────
    st = _styles()

    doc = BaseDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=MARGIN,
        rightMargin=MARGIN,
        topMargin=HEADER_HEIGHT + 0.3 * inch,
        bottomMargin=0.6 * inch,
    )
    frame = Frame(
        MARGIN, doc.bottomMargin,
        CONTENT_W, PAGE_H - HEADER_HEIGHT - 0.3 * inch - doc.bottomMargin,
        id="main",
    )
    doc.addPageTemplates([PageTemplate(
        id="main_page",
        frames=[frame],
        onPage=_make_page_callback(suburb, listing_type),
    )])

    story = []

    # ── Title block ──────────────────────────────────────────────────────────
    story.append(Paragraph("PROPERTY INTELLIGENCE REPORT", st["title_main"]))
    story.append(Paragraph(suburb, st["title_suburb"]))
    # Prominent listing-type badge — visible and clear
    story.append(ListingTypeBadge(listing_type, CONTENT_W))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        f"Generated {date.today().strftime('%d %B %Y')}  |  Report ID: {report_id}",
        st["generated"],
    ))
    story.append(HRFlowable(width=CONTENT_W, thickness=1, color=TEAL, spaceAfter=10))

    # ── Section 1: Market Overview ───────────────────────────────────────────
    source_count = _get_source_count(suburb, listing_type)
    story.append(SectionHeading("MARKET OVERVIEW", CONTENT_W))
    story.append(Spacer(1, 6))
    story.append(_stat_grid(snap, st, source_count=source_count))
    story.append(Spacer(1, 4))

    # Bedroom data warning
    if snap.get("records_with_bedrooms_pct", 100) < 50:
        story.append(Paragraph(
            "Note: Bedroom data is incomplete for this suburb. "
            "Price-per-bedroom figures may not be representative.",
            st["note_grey"],
        ))

    # Stale listing count warning (DOM Fix 2 — Scenario B)
    over_1yr = snap.get("over_one_year_count", 0)
    if over_1yr > 0:
        story.append(Paragraph(
            f"{over_1yr} listing{'s' if over_1yr > 1 else ''} in this suburb "
            f"{'have' if over_1yr > 1 else 'has'} been active for over 1 year "
            f"and may be outdated. Focus on listings under 90 days for the most "
            f"accurate market picture.",
            st["note_orange"],
        ))

    if snap.get("data_freshness"):
        story.append(Paragraph(
            f"Data current as of {snap['data_freshness']}",
            st["note_grey"],
        ))

    story.append(Spacer(1, 10))

    # ── Section 2: Price Trend ───────────────────────────────────────────────
    story.append(SectionHeading("90-DAY PRICE MOVEMENT", CONTENT_W))
    story.append(Spacer(1, 6))

    if len(trend) >= 4:
        story.append(Image(chart_path, width=CONTENT_W, height=CONTENT_W * 3 / 7))
        interp = _trend_interpretation(trend, snap)
        if interp:
            story.append(Spacer(1, 4))
            story.append(Paragraph(interp, st["note_grey"]))
    else:
        no_trend = Table(
            [[Paragraph(
                "Trend data unavailable — insufficient historical data for this suburb",
                st["grey_box"],
            )]],
            colWidths=[CONTENT_W],
        )
        no_trend.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (0, 0), colors.HexColor("#F0F0F0")),
            ("TOPPADDING",    (0, 0), (0, 0), 20),
            ("BOTTOMPADDING", (0, 0), (0, 0), 20),
            ("LEFTPADDING",   (0, 0), (0, 0), 10),
            ("RIGHTPADDING",  (0, 0), (0, 0), 10),
        ]))
        story.append(no_trend)

    story.append(Spacer(1, 14))

    # ── Section 3: Comparable Listings ──────────────────────────────────────
    story.append(SectionHeading("COMPARABLE LISTINGS", CONTENT_W))
    story.append(Spacer(1, 4))

    if fewer_than_3:
        story.append(Paragraph(
            "Fewer than 3 listings match your exact criteria. "
            "Showing all available listings for this suburb.",
            st["note_grey"],
        ))
    else:
        story.append(Paragraph(
            "Current listings matching your criteria. All prices are asking prices.",
            st["note_grey"],
        ))
    story.append(Spacer(1, 4))

    if comps:
        story.append(_comps_table(comps, st))
        if any(c.get("is_stale") for c in comps):
            story.append(Spacer(1, 4))
            story.append(Paragraph(
                "* Listed 90+ days — seller may be open to negotiation. "
                "Orange rows have been on market over 90 days.",
                st["note_orange"],
            ))
    else:
        story.append(Paragraph("No listings found for this suburb.", st["note_grey"]))

    story.append(Spacer(1, 14))

    # ── Section 4: Agent Directory ───────────────────────────────────────────
    story.append(SectionHeading(f"ACTIVE AGENTS IN {suburb.upper()}", CONTENT_W))
    story.append(Spacer(1, 6))

    if agents:
        story.append(_agents_table(agents, st))
        story.append(Spacer(1, 4))
        story.append(Paragraph(
            "Agent listing counts reflect current active listings only.",
            st["note_grey"],
        ))
    else:
        story.append(Paragraph("No agent data available for this suburb.", st["note_grey"]))

    story.append(Spacer(1, 14))

    # ── Section 5: Buyer Advisory ────────────────────────────────────────────
    story.append(SectionHeading("WHAT THE DATA TELLS YOU", CONTENT_W))
    story.append(Spacer(1, 6))

    bullets = _advisory_bullets(snap, trend, comps)
    advisory = Table(
        [[[ Paragraph(f"• &nbsp; {b}", st["bullet"]) for b in bullets ]]],
        colWidths=[CONTENT_W],
    )
    advisory.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (0, 0), LIGHT_NAVY),
        ("TOPPADDING",  (0, 0), (0, 0), 10),
        ("BOTTOMPADDING", (0, 0), (0, 0), 10),
        ("LEFTPADDING", (0, 0), (0, 0), 14),
        ("RIGHTPADDING",(0, 0), (0, 0), 14),
        ("LINEBEFORE",  (0, 0), (0, 0), 3, NAVY),
    ]))
    story.append(KeepTogether(advisory))
    story.append(Spacer(1, 14))

    # ── Section 6: Disclaimer ────────────────────────────────────────────────
    story.append(HRFlowable(width=CONTENT_W, thickness=0.5, color=GREY_MID, spaceAfter=6))
    story.append(Paragraph(
        "This report is produced by Vamba Data and is based on publicly available "
        "asking price data scraped from Zimbabwe property portals. Prices shown are "
        "listing prices only — actual transaction/sale prices are not publicly disclosed "
        "in Zimbabwe and are not reflected here. Data is provided for informational "
        "purposes only and does not constitute financial, legal, or investment advice. "
        "Vamba Data makes no warranty as to the accuracy, completeness, or timeliness "
        "of the information. Always conduct independent due diligence before making any "
        "property decision.",
        st["disclaimer"],
    ))
    story.append(Spacer(1, 4))
    story.append(Paragraph("Vamba Data | vambadata.com", st["centre_grey"]))

    # ── Build ────────────────────────────────────────────────────────────────
    doc.build(story)
    try:
        os.unlink(chart_path)
    except OSError:
        pass

    return output_path


# ── CLI test ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    out_dir = Path(__file__).parent / "output"

    for lt, report_id in [("sale", "TEST-SALE-001"), ("rent", "TEST-RENT-001")]:
        req = {
            "suburb_name":  "Borrowdale",
            "listing_type": lt,
            "property_type": None,
            "bedrooms":     3,
            "budget_min":   None,
            "budget_max":   None,
            "buyer_email":  "test@test.com",
            "report_id":    report_id,
        }
        out_path = str(out_dir / f"{report_id}.pdf")
        print(f"\n{'='*55}")
        print(f"Generating {report_id} ({lt.upper()}, 3-bed, Borrowdale)...")

        snap  = get_suburb_snapshot("Borrowdale", bedrooms=3, listing_type=lt)
        comps = get_comparable_listings("Borrowdale", bedrooms=3, listing_type=lt, limit=8)
        trend = get_price_trend("Borrowdale", weeks=12, listing_type=lt)
        agents = get_active_agents("Borrowdale", limit=5)

        print(f"  active_listing_count : {snap['active_listing_count']}")
        print(f"  median_asking_price  : {snap['median_asking_price']}")
        print(f"  avg_asking_price     : {snap['avg_asking_price']}")
        print(f"  avg_dom (all)        : {snap['avg_days_on_market']}")
        print(f"  avg_dom (excl stale) : {snap['avg_dom_excl_stale']}")
        print(f"  over_one_year_count  : {snap['over_one_year_count']}")
        print(f"  market_temperature   : {snap['market_temperature']}")
        print(f"  comparable listings  : {len(comps)}")
        print(f"  trend weeks          : {len(trend)}")
        print(f"  agents               : {len(agents)}")

        generate_report(req, out_path)
        size = Path(out_path).stat().st_size
        print(f"  PDF: {out_path}  ({size // 1024} KB)")

    # Available suburbs with counts
    from analytics.suburb_queries import get_available_suburbs
    print(f"\n{'='*55}")
    print("Available suburbs (sale listing count):")
    suburbs = get_available_suburbs(with_counts=True)
    print(f"  Total qualifying: {len(suburbs)}")
    print(f"  {'suburb':<28} {'sale':>6} {'rent':>6}")
    print(f"  {'-'*28} {'-'*6} {'-'*6}")
    for s in suburbs:
        print(f"  {s['suburb_name']:<28} {s['sale_listing_count']:>6} "
              f"{s['rental_listing_count']:>6}")
