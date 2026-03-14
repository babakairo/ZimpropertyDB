"""
reports/social_content.py
Auto-generates weekly social media post drafts from Snowflake data.

Run after weekly_report.py:
    python reports/social_content.py

Output: reports/output/social_posts_YYYY-MM-DD.txt
"""
import os
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

WEEK = date.today().strftime("%d %B %Y")
OUT  = Path(__file__).parent / "output" / f"social_posts_{date.today().isoformat()}.txt"

CTA_LINK = "https://wa.me/447459920895"


def get_connection():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "ZIM_PROPERTY_DB"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ZIM_PROPERTY_WH"),
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


def fetch_one(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchone()


def fetch_all(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def fmt(val, prefix="$"):
    """Format a number with comma separators."""
    if val is None:
        return "—"
    return f"{prefix}{val:,.0f}" if prefix else f"{val:,.0f}"


def generate_posts(cursor) -> list[str]:
    posts = []

    # ── Data pulls ─────────────────────────────────────────────────────────────

    top5_stands = fetch_all(cursor, """
        SELECT suburb_clean, ROUND(AVG(price_per_sqm_usd), 0) AS avg_ppsqm
        FROM ANALYTICS.LAND_LISTINGS
        WHERE price_per_sqm_usd IS NOT NULL AND city_clean ILIKE '%Harare%'
        GROUP BY suburb_clean HAVING COUNT(*) >= 2
        ORDER BY avg_ppsqm DESC LIMIT 5
    """)

    best_value = fetch_all(cursor, """
        SELECT suburb_clean, ROUND(AVG(price_per_sqm_usd), 0) AS avg_ppsqm
        FROM ANALYTICS.LAND_LISTINGS
        WHERE price_per_sqm_usd IS NOT NULL AND city_clean ILIKE '%Harare%'
        GROUP BY suburb_clean HAVING COUNT(*) >= 2
        ORDER BY avg_ppsqm ASC LIMIT 3
    """)

    three_bed = fetch_one(cursor, """
        SELECT ROUND(AVG(property_price_usd), 0)
        FROM ANALYTICS.HOUSE_SALE_LISTINGS
        WHERE number_of_bedrooms = 3
          AND city_clean ILIKE '%Harare%'
          AND property_price_usd IS NOT NULL
    """)

    top_growth = fetch_all(cursor, """
        SELECT suburb_clean, ROUND(growth_6m_pct, 1) AS growth
        FROM ANALYTICS.SUBURB_PRICE_GROWTH
        WHERE growth_6m_pct IS NOT NULL
        ORDER BY growth_6m_pct DESC LIMIT 3
    """)

    # ── New: weekly movers from SUBURB_MARKET_STATS ──────────────────────────
    weekly_risers = fetch_all(cursor, """
        SELECT suburb_clean, city_clean,
               ROUND(avg_price_usd, 0) AS avg_price,
               ROUND(wow_price_change_pct, 1) AS wow_pct
        FROM ANALYTICS.SUBURB_MARKET_STATS
        WHERE wow_price_change_pct IS NOT NULL
          AND wow_price_change_pct > 0
          AND city_clean ILIKE '%Harare%'
          AND listing_type = 'sale'
          AND property_type = 'house'
          AND listing_count >= 3
          AND week_start = DATE_TRUNC('week', CURRENT_DATE())
        ORDER BY wow_pct DESC
        LIMIT 3
    """) or []

    weekly_fallers = fetch_all(cursor, """
        SELECT suburb_clean, ROUND(avg_price_usd, 0) AS avg_price,
               ROUND(wow_price_change_pct, 1) AS wow_pct
        FROM ANALYTICS.SUBURB_MARKET_STATS
        WHERE wow_price_change_pct IS NOT NULL
          AND wow_price_change_pct < 0
          AND city_clean ILIKE '%Harare%'
          AND listing_type = 'sale'
          AND listing_count >= 3
          AND week_start = DATE_TRUNC('week', CURRENT_DATE())
        ORDER BY wow_pct ASC
        LIMIT 3
    """) or []

    # New listings this week vs last week for market pulse
    new_vs_last = fetch_one(cursor, """
        SELECT
            SUM(CASE WHEN week_start = DATE_TRUNC('week', CURRENT_DATE()) THEN new_listings ELSE 0 END) AS new_this_week,
            SUM(CASE WHEN week_start = DATE_TRUNC('week', DATEADD('week', -1, CURRENT_DATE())) THEN new_listings ELSE 0 END) AS new_last_week
        FROM ANALYTICS.SUBURB_MARKET_STATS
        WHERE city_clean ILIKE '%Harare%'
          AND listing_type = 'sale'
    """) or (None, None)

    # Price drop opportunities from master (when matching engine has run)
    price_drops = fetch_all(cursor, """
        SELECT pm.canonical_address, pm.suburb, pm.city,
               pm.current_price_usd, pm.min_price_usd,
               ROUND((pm.current_price_usd - pm.max_price_usd) / NULLIF(pm.max_price_usd,0) * 100, 1) AS drop_from_peak
        FROM MASTER.PROPERTY_MASTER pm
        WHERE pm.current_price_usd < pm.max_price_usd * 0.90
          AND pm.is_currently_active = TRUE
          AND pm.current_price_usd IS NOT NULL
          AND pm.max_price_usd IS NOT NULL
          AND LOWER(pm.city) LIKE '%harare%'
        ORDER BY drop_from_peak ASC
        LIMIT 3
    """) or []

    top_yield = fetch_all(cursor, """
        SELECT suburb_clean, ROUND(gross_rental_yield_pct, 1) AS yield_pct
        FROM ANALYTICS.RENTAL_YIELD_BY_SUBURB
        WHERE gross_rental_yield_pct IS NOT NULL
        ORDER BY gross_rental_yield_pct DESC LIMIT 3
    """)

    totals = fetch_one(cursor, """
        SELECT COUNT(*), COUNT(DISTINCT suburb_clean), COUNT(DISTINCT city_clean)
        FROM STAGING.CLEANED_PROPERTY_LISTINGS
    """)

    avg_rent = fetch_one(cursor, """
        SELECT ROUND(AVG(monthly_rent_usd), 0)
        FROM ANALYTICS.RENTAL_LISTINGS
        WHERE monthly_rent_usd IS NOT NULL AND city_clean ILIKE '%Harare%'
    """)

    # ── MONDAY — Market Pulse ──────────────────────────────────────────────────
    stand_lines = "\n".join(
        f"• {r[0]}: {fmt(r[1])}/sqm avg" for r in top5_stands
    ) if top5_stands else "• Data being compiled..."

    top_suburb   = top5_stands[0][0]  if top5_stands  else "premium suburbs"
    top_ppsqm    = top5_stands[0][1]  if top5_stands  else 0
    total_props  = f"{totals[0]:,}"   if totals        else "—"
    total_suburbs = totals[1]         if totals        else "—"
    total_cities  = totals[2]         if totals        else "—"

    new_this_week = new_vs_last[0] if new_vs_last and new_vs_last[0] else None
    new_last_week = new_vs_last[1] if new_vs_last and new_vs_last[1] else None
    new_listings_line = ""
    if new_this_week is not None:
        delta = ""
        if new_last_week and new_last_week > 0:
            chg = round((new_this_week - new_last_week) / new_last_week * 100)
            delta = f" ({'▲' if chg > 0 else '▼'}{abs(chg)}% vs last week)"
        new_listings_line = f"\n📬 New listings this week: {new_this_week:,}{delta}"

    posts.append(f"""\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MONDAY — MARKET PULSE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏠 ZIMBABWE PROPERTY MARKET — WEEK OF {WEEK}

This week we tracked {total_props} properties across \
{total_suburbs} suburbs in {total_cities} cities.{new_listings_line}

📊 HARARE STAND PRICES THIS WEEK:
{stand_lines}

💡 A 500sqm stand in {top_suburb} would cost approximately \
{fmt(top_ppsqm * 500)} at current market rates.

Know the numbers before you negotiate.
📥 Full weekly report → {CTA_LINK}

#ZimbabweProperty #HarareProperty #ZimDiaspora #PropertyZimbabwe
""")

    # ── TUESDAY — Suburb Spotlight ────────────────────────────────────────────
    if top5_stands:
        s_name  = top5_stands[0][0]
        s_ppsqm = top5_stands[0][1]
        posts.append(f"""\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TUESDAY — SUBURB SPOTLIGHT: {s_name.upper()}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📍 SUBURB SPOTLIGHT: {s_name}, HARARE

This week's market data:

💰 Average stand price:  {fmt(s_ppsqm)}/sqm
📐 What a fair stand looks like:
   • 500sqm  → ~{fmt(s_ppsqm * 500)}
   • 750sqm  → ~{fmt(s_ppsqm * 750)}
   • 1,000sqm → ~{fmt(s_ppsqm * 1000)}

If you are being quoted significantly more than this,
ask your agent to justify the premium.

📩 DM us "{s_name.upper()}" for the full suburb breakdown.
🔗 Full market report → {CTA_LINK}

#Harare{s_name.replace(" ", "")} #ZimProperty #StandPrices
""")
    else:
        posts.append("TUESDAY — SUBURB SPOTLIGHT\n[Insufficient stand data this week]\n")

    # ── WEDNESDAY — Price Check ────────────────────────────────────────────────
    if best_value:
        bv_name   = best_value[0][0]
        bv_ppsqm  = best_value[0][1]
        bv_lines  = "\n".join(
            f"{i+1}. {r[0]}: {fmt(r[1])}/sqm avg" for i, r in enumerate(best_value)
        )
        posts.append(f"""\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WEDNESDAY — PRICE CHECK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ OVERPRICED OR FAIR? HERE'S HOW TO TELL.

The most affordable stands in Harare this week:

{bv_lines}

📐 HOW TO USE THIS:
Stand size (sqm) × price per sqm = fair market value.

Example:
A 500sqm stand in {bv_name} should cost ~{fmt(bv_ppsqm * 500)}.
If you're quoted more, negotiate hard — or walk away.

🔗 Weekly price report → {CTA_LINK}

Tag someone who needs to see this before buying a stand 👇

#PropertyZimbabwe #StandPrices #ZimDiaspora #KnowBeforeYouBuy
""")
    else:
        posts.append("WEDNESDAY — PRICE CHECK\n[Insufficient land data this week]\n")

    # ── THURSDAY — Investment Signal ───────────────────────────────────────────
    if top_growth or weekly_risers:
        growth_lines = "\n".join(
            f"{i+1}. {r[0]} — up {r[1]}% over 6 months" for i, r in enumerate(top_growth)
        ) if top_growth else ""
        fastest = top_growth[0][0] if top_growth else (weekly_risers[0][0] if weekly_risers else "—")

        weekly_movers_section = ""
        if weekly_risers:
            riser_lines = "\n".join(
                f"↑ {r[0]}: +{r[3]}% this week (avg {fmt(r[2])})" for r in weekly_risers
            )
            weekly_movers_section = f"\n\n📅 THIS WEEK'S MOVERS:\n{riser_lines}"

        price_drop_section = ""
        if price_drops:
            drop_lines = "\n".join(
                f"• {r[1]}, {r[2]}: now {fmt(r[3])} ({r[5]}% below peak)" for r in price_drops
            )
            price_drop_section = f"\n\n🏷️ PRICE REDUCTIONS THIS WEEK:\n{drop_lines}"

        posts.append(f"""\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THURSDAY — INVESTMENT SIGNAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 SUBURBS WITH RISING PROPERTY PRICES IN ZIMBABWE

Based on 6-month trend data from our database:

{growth_lines}{weekly_movers_section}{price_drop_section}

Early buyers in {fastest} are already sitting on paper gains.
Those who waited are paying more today.

⚠️ Market data only. Always do your own due diligence.

📊 Full growth data for all suburbs → {CTA_LINK}

#ZimPropertyInvestment #HarareProperty #PropertyTrends #ZimDiaspora
""")
    else:
        posts.append("THURSDAY — INVESTMENT SIGNAL\n[Insufficient growth trend data this week]\n")

    # ── FRIDAY — Data Fact ────────────────────────────────────────────────────
    avg_3bed_str  = fmt(three_bed[0]) if three_bed and three_bed[0] else "—"
    avg_rent_str  = fmt(avg_rent[0])  if avg_rent  and avg_rent[0]  else "—"
    cheap_suburb  = best_value[0][0]  if best_value  else "—"
    cheap_ppsqm   = fmt(best_value[0][1]) if best_value else "—"
    prem_suburb   = top5_stands[0][0] if top5_stands else "—"
    prem_ppsqm    = fmt(top5_stands[0][1]) if top5_stands else "—"

    posts.append(f"""\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FRIDAY — DATA FACT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 ZIMBABWE PROPERTY FACTS — WEEK OF {WEEK}

🏡 Avg 3-bed house price in Harare:  {avg_3bed_str}
🏘️ Avg monthly rent in Harare:       {avg_rent_str}/month
📍 Best value stand suburb:          {cheap_suburb} at {cheap_ppsqm}/sqm
🏆 Most expensive stand suburb:      {prem_suburb} at {prem_ppsqm}/sqm

The price gap between the cheapest and most expensive
suburb for stands shows how much location matters.

Know which suburb matches your budget before you start shopping.
📥 Full suburb breakdown → {CTA_LINK}

Save this. You'll need it 💾

#ZimbabweProperty #PropertyFacts #HarareHouses #ZimPropertyData
""")

    # ── SATURDAY — Diaspora Special ───────────────────────────────────────────
    top3_lines = "\n".join(
        f"• {r[0]}: {fmt(r[1])}/sqm" for r in top5_stands[:3]
    ) if top5_stands else "• Data available in full report"

    posts.append(f"""\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SATURDAY — DIASPORA SPECIAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🌍 ZIMBABWEANS BUYING PROPERTY FROM ABROAD — READ THIS

Before you wire money home for a property purchase,
here are this week's market benchmarks for Harare stands:

{top3_lines}

📐 Simple formula to protect yourself:
   Stand size (sqm) × price per sqm = what you should pay.
   Anything significantly above this = overpriced.

We have seen diaspora buyers overpay by $10,000–$50,000
on a single transaction — simply because they had no data.

Our weekly report costs a fraction of being overcharged once.

📩 Join Zimbabwe investors getting weekly market data:
{CTA_LINK}

Share this with your Zimbabwe family WhatsApp group 🙏

#ZimDiaspora #ZimbabweDiaspora #PropertyZimbabwe #BuyingFromAbroad
""")

    # ── SUNDAY — Weekly Summary ────────────────────────────────────────────────
    fastest_growth  = f"{top_growth[0][0]} (+{top_growth[0][1]}% / 6 months)" if top_growth else "—"
    best_yield_str  = f"{top_yield[0][0]} ({top_yield[0][1]}% gross yield)"   if top_yield  else "—"
    aff_stands_str  = f"{best_value[0][0]} at {fmt(best_value[0][1])}/sqm"    if best_value else "—"

    posts.append(f"""\
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SUNDAY — WEEKLY MARKET SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 ZIMBABWE PROPERTY WEEKLY WRAP — {WEEK}

This week in numbers:
🏘️ {total_props} properties tracked
📍 {total_suburbs} suburbs covered
🏙️ {total_cities} cities monitored

🔑 THIS WEEK'S HIGHLIGHTS:
✅ Most affordable stands:   {aff_stands_str}
📈 Fastest-growing suburb:  {fastest_growth}
💼 Best rental yield:       {best_yield_str}
🏡 Avg 3-bed house (Harare): {avg_3bed_str}

📊 Get the full detailed weekly report:
All suburbs · All prices · Agent contacts · Price trends

🔗 {CTA_LINK}  |  First report FREE

#ZimPropertyMarket #WeeklyWrap #HarareProperty #ZimbabweProperty
""")

    return posts


def main():
    print("Connecting to Snowflake ...")
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        posts = generate_posts(cursor)
        OUT.parent.mkdir(parents=True, exist_ok=True)
        separator = "\n" + ("=" * 60) + "\n\n"
        OUT.write_text(
            f"ZIMBABWE PROPERTY — SOCIAL MEDIA POSTS\nWeek of {WEEK}\n"
            + ("=" * 60) + "\n\n"
            + separator.join(posts),
            encoding="utf-8",
        )
        print(f"\n[OK] {len(posts)} posts generated -> {OUT}\n")
        for p in posts:
            first_line = p.strip().splitlines()[1] if p.strip() else ""
            print(f"  - {first_line}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
