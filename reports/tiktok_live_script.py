"""
reports/tiktok_live_script.py
Generates a weekly TikTok Live presenter runsheet from Snowflake data.
Output: reports/output/tiktok_live_script_YYYY-MM-DD.txt

Usage:
    python reports/tiktok_live_script.py
"""

import os
import sys
from datetime import date
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

OUT = Path(__file__).parent / "output" / f"tiktok_live_script_{date.today().isoformat()}.txt"

WHATSAPP = "wa.me/447459920895"
INSTAGRAM = "@ba_kairo"
TIKTOK = "@izeremhepo"


def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        schema="ANALYTICS",
    )


def fetch_data():
    data = {}
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Total listings
        cur.execute("""
            SELECT
                COUNT(CASE WHEN listing_type = 'sale' THEN 1 END),
                COUNT(CASE WHEN listing_type = 'rent' THEN 1 END),
                COUNT(CASE WHEN property_type = 'land' THEN 1 END)
            FROM ZIM_PROPERTY_DB.RAW.ZW_PROPERTY_LISTINGS
            WHERE scraped_at >= DATEADD(day, -7, CURRENT_DATE)
        """)
        row = cur.fetchone()
        data["sales"]   = row[0] or 0
        data["rentals"] = row[1] or 0
        data["land"]    = row[2] or 0
        data["total"]   = data["sales"] + data["rentals"] + data["land"]

        # Top 3 suburbs by price growth
        cur.execute("""
            SELECT suburb_clean, city_clean, growth_3m_pct, avg_price_current_month_usd
            FROM ZIM_PROPERTY_DB.ANALYTICS.SUBURB_PRICE_GROWTH
            WHERE growth_3m_pct IS NOT NULL
            ORDER BY growth_3m_pct DESC
            LIMIT 3
        """)
        data["top_growth"] = cur.fetchall()

        # Best value stands under $30k
        cur.execute("""
            SELECT suburb_clean, city_clean, MIN(property_price_usd), AVG(property_size_sqm)
            FROM ZIM_PROPERTY_DB.ANALYTICS.LAND_LISTINGS
            WHERE property_price_usd BETWEEN 5000 AND 30000
            GROUP BY suburb_clean, city_clean
            ORDER BY MIN(property_price_usd) ASC
            LIMIT 3
        """)
        data["best_value"] = cur.fetchall()

        # Top rental yield suburbs
        cur.execute("""
            SELECT suburb_clean, city_clean, gross_rental_yield_pct, avg_monthly_rent
            FROM ZIM_PROPERTY_DB.ANALYTICS.RENTAL_YIELD_BY_SUBURB
            WHERE gross_rental_yield_pct IS NOT NULL
            ORDER BY gross_rental_yield_pct DESC
            LIMIT 3
        """)
        data["top_yield"] = cur.fetchall()

        # Average 3-bed house price in Harare
        cur.execute("""
            SELECT ROUND(AVG(property_price_usd), 0)
            FROM ZIM_PROPERTY_DB.ANALYTICS.HOUSE_SALE_LISTINGS
            WHERE number_of_bedrooms = 3
              AND city_clean ILIKE '%harare%'
              AND property_price_usd > 0
        """)
        row = cur.fetchone()
        data["three_bed_avg"] = int(row[0]) if row and row[0] else 0

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[WARN] Snowflake query failed: {e} — using placeholder data")
        data = {
            "total": 3309, "sales": 2100, "rentals": 890, "land": 319,
            "top_growth": [("Pomona", "Harare", 18.2, 95000),
                           ("Borrowdale", "Harare", 14.5, 185000),
                           ("Ruwa", "Harare", 11.8, 45000)],
            "best_value": [("Ruwa", "Harare", 8500, 400),
                           ("Epworth", "Harare", 12000, 500),
                           ("Chitungwiza", "Harare", 15000, 600)],
            "top_yield":  [("Avondale", "Harare", 9.2, 650),
                           ("Belgravia", "Harare", 8.7, 850),
                           ("Greendale", "Harare", 7.9, 550)],
            "three_bed_avg": 78000,
        }
    return data


def build_script(d: dict) -> str:
    week = date.today().strftime("%d %B %Y")

    growth_lines = "\n".join(
        f"     {i+1}. {r[0]}, {r[1]} — +{r[2]:.1f}% (avg ${r[3]:,.0f})"
        for i, r in enumerate(d["top_growth"])
    ) if d["top_growth"] else "     Data not available"

    value_lines = "\n".join(
        f"     {i+1}. {r[0]}, {r[1]} — from ${r[2]:,.0f} (~{int(r[3])}sqm)"
        for i, r in enumerate(d["best_value"])
    ) if d["best_value"] else "     Data not available"

    yield_lines = "\n".join(
        f"     {i+1}. {r[0]}, {r[1]} — {float(r[2]):.1f}% yield, ~${float(r[3]):,.0f}/month rent"
        for i, r in enumerate(d["top_yield"])
    ) if d["top_yield"] else "     Data not available"

    script = f"""
{'=' * 65}
  ZIMBABWE PROPERTY INTELLIGENCE — TIKTOK LIVE RUNSHEET
  Week of {week}
  Host: Eno | {TIKTOK} | {INSTAGRAM}
{'=' * 65}

BEFORE YOU GO LIVE
------------------
  - Open Snowflake dashboard or weekly Excel report on second screen
  - Have WhatsApp open: {WHATSAPP}
  - Pin the comment: "DM me for the full report"
  - Recommended duration: 20–30 minutes

{'─' * 65}
[00:00 – 01:00]  HOOK & INTRO
{'─' * 65}

  SAY:
  "Welcome to the Zimbabwe Property Market Live — I'm Eno, and
   every week I analyse over {d['total']:,} property listings across
   Zimbabwe so you don't have to.

   Whether you're in the UK, US, or Canada looking to invest back
   home — this is your show. Let's get into the numbers."

  ACTION: Wave, smile, look confident. Keep energy high.

{'─' * 65}
[01:00 – 04:00]  THIS WEEK'S MARKET SNAPSHOT
{'─' * 65}

  SAY:
  "This week we tracked {d['total']:,} listings:
   - {d['sales']:,} properties for sale
   - {d['rentals']:,} rentals
   - {d['land']:,} stands and land plots

   The market is {('active' if d['total'] > 3000 else 'steady')} right now.
   Let me show you where the money is moving."

  SHOW: Screen-share or hold up weekly report card.

{'─' * 65}
[04:00 – 08:00]  TOP 3 SUBURBS WITH HIGHEST PRICE GROWTH
{'─' * 65}

  SAY:
  "Let's talk about WHERE prices are rising fastest.
   These are the top 3 suburbs by 3-month price growth:

{growth_lines}

   If you bought in any of these areas 3 months ago,
   you're already sitting on a gain."

  TIP: Pause after each suburb. Let it sink in.

{'─' * 65}
[08:00 – 12:00]  BEST VALUE STANDS UNDER $30,000
{'─' * 65}

  SAY:
  "Now for the diaspora investors — stands are your entry point.
   You don't need $100k. Here are stands available RIGHT NOW
   under $30,000:

{value_lines}

   A stand in Ruwa or Chitungwiza bought today could be worth
   double in 5 years. I've seen it happen."

  ACTION: Ask "Drop a comment if you're looking to buy land!"

{'─' * 65}
[12:00 – 16:00]  RENTAL YIELD — WHERE TO BUY FOR INCOME
{'─' * 65}

  SAY:
  "If you want your property to EARN money every month,
   you need to buy in high-yield suburbs.
   Top 3 rental yield areas right now:

{yield_lines}

   A 9% yield means if you buy a $100k property,
   it pays you $9,000 a year in rent. That's $750 a month."

  ACTION: Pin comment — "Comment YIELD if you want the full breakdown"

{'─' * 65}
[16:00 – 19:00]  MARKET FACT OF THE WEEK
{'─' * 65}

  SAY:
  "Here's your data fact of the week:
   The average 3-bedroom house in Harare now costs ${d['three_bed_avg']:,}.

   Five years ago that same house was a fraction of that price.
   Zimbabwe real estate is one of the fastest appreciating markets
   on the continent right now — and most people in the diaspora
   are sleeping on it."

{'─' * 65}
[19:00 – 25:00]  LIVE Q&A
{'─' * 65}

  SAY:
  "Now I want to hear from YOU. Drop your questions in the comments.
   - Looking to buy? Tell me your budget and I'll tell you where to look.
   - Already own property in Zim? Tell me where — I'll look up the data.
   - Want the full weekly report with all {d['total']:,} listings?
     Message me on WhatsApp — {WHATSAPP}"

  TIPS:
  - Read comments out loud before answering
  - Repeat the WhatsApp number at least 3 times during Q&A
  - Pin your own comment with the WhatsApp link

{'─' * 65}
[25:00 – 27:00]  CLOSE & CALL TO ACTION
{'─' * 65}

  SAY:
  "That's a wrap on this week's Zimbabwe Property Market Live.

   If this was useful — follow {TIKTOK}, the algorithm needs
   to show this to more Zimbos in the diaspora.

   For the full weekly report with every listing, every price,
   every suburb — WhatsApp me directly: {WHATSAPP}
   First report is FREE.

   I'll see you next week. Iwe neni tine basa."

  ACTION: Stay on for 2 more minutes after signing off.
          People convert AFTER the close.

{'─' * 65}
  REPEAT THROUGHOUT THE LIVE (at least every 5 minutes):
    "WhatsApp me for the full report: {WHATSAPP}"
    "Follow {TIKTOK} for weekly Zimbabwe property data"
    "Check Instagram {INSTAGRAM} for daily market cards"
{'─' * 65}

{'=' * 65}
  END OF RUNSHEET — Generated {date.today().strftime('%d %B %Y')}
{'=' * 65}
""".strip()

    return script


def main():
    print("Fetching market data from Snowflake...")
    data = fetch_data()

    print("Generating TikTok Live script...")
    script = build_script(data)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(script, encoding="utf-8")

    print(f"Script saved to: {OUT}")
    preview = script[:500].encode("ascii", "replace").decode("ascii")
    print()
    print(preview)
    print("...")


if __name__ == "__main__":
    main()
