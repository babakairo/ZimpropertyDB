from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import snowflake.connector
from dotenv import load_dotenv
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "configs" / ".env")


def _connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.getenv("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
        role=os.getenv("SNOWFLAKE_ROLE", "ZIM_ANALYST_ROLE"),
    )


def _fetch_rows(cur, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_suburb_report_data(suburb: str, city: str | None = None) -> dict[str, Any]:
    conn = _connect()
    cur = conn.cursor()
    try:
        city_filter = ""
        params: list[Any] = [suburb.lower()]
        if city:
            city_filter = " AND LOWER(city_clean) = %s "
            params.append(city.lower())

        summary_sql = f"""
            SELECT
                week_start,
                city_clean,
                suburb_clean,
                listing_count,
                avg_price_usd,
                median_price_usd,
                avg_price_per_sqm_usd,
                avg_price_per_bedroom_usd,
                avg_days_on_market,
                wow_price_change_pct,
                mom_price_change_pct,
                yoy_price_change_pct
            FROM ZIM_PROPERTY_DB.ANALYTICS.SUBURB_MARKET_STATS
            WHERE LOWER(suburb_clean) = %s
              {city_filter}
              AND listing_type = 'sale'
            ORDER BY week_start DESC
            LIMIT 1
        """
        summary = _fetch_rows(cur, summary_sql, tuple(params))

        bedroom_sql = f"""
            SELECT
                number_of_bedrooms,
                listing_count,
                avg_price_usd,
                median_price_usd,
                avg_price_per_sqm_usd
            FROM ZIM_PROPERTY_DB.ANALYTICS.AVERAGE_PRICE_BY_BEDROOM
            WHERE LOWER(suburb_clean) = %s
              {city_filter}
              AND listing_type = 'sale'
              AND snapshot_month = (
                  SELECT MAX(snapshot_month)
                  FROM ZIM_PROPERTY_DB.ANALYTICS.AVERAGE_PRICE_BY_BEDROOM
              )
            ORDER BY number_of_bedrooms
            LIMIT 10
        """
        bedroom = _fetch_rows(cur, bedroom_sql, tuple(params))

        opportunities_sql = f"""
            SELECT
                canonical_address,
                current_price_usd,
                peak_price_usd,
                drop_from_peak_pct,
                days_on_market,
                listing_url
            FROM ZIM_PROPERTY_DB.ANALYTICS.V_PRICE_DROP_OPPORTUNITIES
            WHERE LOWER(suburb) = %s
              {" AND LOWER(city) = %s " if city else ""}
            ORDER BY drop_from_peak_pct ASC
            LIMIT 8
        """
        opp_params: list[Any] = [suburb.lower()]
        if city:
            opp_params.append(city.lower())
        opportunities = _fetch_rows(cur, opportunities_sql, tuple(opp_params))

        return {
            "suburb": suburb,
            "city": city,
            "generated_at": datetime.utcnow().isoformat(),
            "summary": summary[0] if summary else None,
            "bedroom_table": bedroom,
            "opportunities": opportunities,
        }
    finally:
        cur.close()
        conn.close()


def generate_suburb_pdf(data: dict[str, Any], output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    styles = getSampleStyleSheet()
    story = []

    suburb = data.get("suburb")
    city = data.get("city")
    title = f"Suburb Intelligence Report — {suburb}" + (f", {city}" if city else "")

    story.append(Paragraph(title, styles["Title"]))
    story.append(Paragraph(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}", styles["Normal"]))
    story.append(Spacer(1, 12))

    summary = data.get("summary")
    if summary:
        summary_rows = [
            ["Week Start", str(summary.get("week_start", ""))],
            ["Listings", str(summary.get("listing_count", ""))],
            ["Avg Price (USD)", f"{summary.get('avg_price_usd', 0):,.0f}" if summary.get("avg_price_usd") else "-"],
            ["Median Price (USD)", f"{summary.get('median_price_usd', 0):,.0f}" if summary.get("median_price_usd") else "-"],
            ["Avg Price / SQM", f"{summary.get('avg_price_per_sqm_usd', 0):,.0f}" if summary.get("avg_price_per_sqm_usd") else "-"],
            ["Avg Price / Bedroom", f"{summary.get('avg_price_per_bedroom_usd', 0):,.0f}" if summary.get("avg_price_per_bedroom_usd") else "-"],
            ["Avg Days on Market", f"{summary.get('avg_days_on_market', 0):.1f}" if summary.get("avg_days_on_market") else "-"],
            ["WoW Change %", f"{summary.get('wow_price_change_pct', 0):.2f}" if summary.get("wow_price_change_pct") is not None else "-"],
            ["MoM Change %", f"{summary.get('mom_price_change_pct', 0):.2f}" if summary.get("mom_price_change_pct") is not None else "-"],
            ["YoY Change %", f"{summary.get('yoy_price_change_pct', 0):.2f}" if summary.get("yoy_price_change_pct") is not None else "-"],
        ]
        story.append(Paragraph("Market Summary", styles["Heading2"]))
        table = Table(summary_rows, colWidths=[180, 280])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ]))
        story.append(table)
        story.append(Spacer(1, 12))

    bedroom_rows = data.get("bedroom_table") or []
    story.append(Paragraph("Pricing by Bedroom", styles["Heading2"]))
    if bedroom_rows:
        bed_table_data = [["Bedrooms", "Listings", "Avg USD", "Median USD", "Avg $/SQM"]]
        for row in bedroom_rows:
            bed_table_data.append([
                row.get("number_of_bedrooms"),
                row.get("listing_count"),
                f"{row.get('avg_price_usd', 0):,.0f}" if row.get("avg_price_usd") else "-",
                f"{row.get('median_price_usd', 0):,.0f}" if row.get("median_price_usd") else "-",
                f"{row.get('avg_price_per_sqm_usd', 0):,.0f}" if row.get("avg_price_per_sqm_usd") else "-",
            ])
        bed_table = Table(bed_table_data, colWidths=[70, 70, 90, 90, 90])
        bed_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ]))
        story.append(bed_table)
    else:
        story.append(Paragraph("No bedroom-level rows for selected suburb.", styles["Normal"]))
    story.append(Spacer(1, 12))

    opportunities = data.get("opportunities") or []
    story.append(Paragraph("Price Drop Opportunities", styles["Heading2"]))
    if opportunities:
        opp_data = [["Address", "Current USD", "Peak USD", "Drop %", "DOM"]]
        for row in opportunities:
            opp_data.append([
                (row.get("canonical_address") or "")[:45],
                f"{row.get('current_price_usd', 0):,.0f}" if row.get("current_price_usd") else "-",
                f"{row.get('peak_price_usd', 0):,.0f}" if row.get("peak_price_usd") else "-",
                f"{row.get('drop_from_peak_pct', 0):.1f}" if row.get("drop_from_peak_pct") is not None else "-",
                str(row.get("days_on_market") or "-"),
            ])
        opp_table = Table(opp_data, colWidths=[220, 70, 70, 50, 50])
        opp_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ]))
        story.append(opp_table)
    else:
        story.append(Paragraph("No active price-drop opportunities in this suburb right now.", styles["Normal"]))

    doc = SimpleDocTemplate(str(output), pagesize=A4)
    doc.build(story)
    return output


def build_suburb_report(suburb: str, city: str | None, output_path: str | Path) -> Path:
    data = fetch_suburb_report_data(suburb=suburb, city=city)
    return generate_suburb_pdf(data, output_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate suburb PDF report")
    parser.add_argument("--suburb", required=True)
    parser.add_argument("--city", default=None)
    parser.add_argument("--out", default=str(ROOT / "reports" / "output" / "suburb_report.pdf"))
    args = parser.parse_args()

    out = build_suburb_report(args.suburb, args.city, args.out)
    print(f"Generated: {out}")
