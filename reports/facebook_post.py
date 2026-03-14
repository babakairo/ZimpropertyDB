"""
reports/facebook_post.py
Posts the 7 weekly video cards to a Facebook Page via the Graph API,
scheduled Monday–Sunday at a configured time.

Setup (one-time):
    1. Go to https://developers.facebook.com → create an App (type: Business)
    2. Add "Pages" product, request pages_manage_posts + pages_read_engagement
    3. Generate a Page Access Token (Settings → Token Tools, or Graph API Explorer)
    4. Set FB_PAGE_ID and FB_PAGE_ACCESS_TOKEN in configs/.env

Usage:
    python reports/facebook_post.py --day monday   # post today's card now
    python reports/facebook_post.py --all           # schedule all 7 for the week
    python reports/facebook_post.py --dry-run       # preview without posting
"""
import os
import sys
import time
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

FB_PAGE_ID    = os.getenv("FB_PAGE_ID", "")
FB_PAGE_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN", "")
GRAPH_API     = "https://graph.facebook.com/v19.0"

CARDS_DIR     = Path(__file__).parent / "output" / "cards" / date.today().isoformat()
POSTS_FILE    = Path(__file__).parent / "output" / f"social_posts_{date.today().isoformat()}.txt"

# Post time: 10:00 AM Zimbabwe time (UTC+2) = 08:00 UTC
POST_HOUR_UTC = 8

# Day → card slug + caption section header
DAY_MAP = {
    "monday":    ("monday_market_pulse",        "MONDAY — MARKET PULSE"),
    "tuesday":   ("tuesday_suburb_spotlight",   "TUESDAY — SUBURB SPOTLIGHT"),
    "wednesday": ("wednesday_price_check",      "WEDNESDAY — PRICE CHECK"),
    "thursday":  ("thursday_investment_signal", "THURSDAY — INVESTMENT SIGNAL"),
    "friday":    ("friday_data_fact",           "FRIDAY — DATA FACT"),
    "saturday":  ("saturday_diaspora_special",  "SATURDAY — DIASPORA SPECIAL"),
    "sunday":    ("sunday_weekly_summary",      "SUNDAY — WEEKLY MARKET SUMMARY"),
}


# ── Caption extractor ─────────────────────────────────────────────────────────
def get_caption(day: str) -> str:
    """Extract the caption for the given day from the social_posts file."""
    if not POSTS_FILE.exists():
        return f"Zimbabwe Property Market Intelligence — Week of {date.today().strftime('%d %B %Y')}"

    _, section_header = DAY_MAP[day]
    text   = POSTS_FILE.read_text(encoding="utf-8")
    marker = f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{section_header}"

    start = text.find(marker)
    if start == -1:
        return f"Zimbabwe Property Intelligence — {section_header}"

    # Find the content after the second ━━━ line
    content_start = text.find("\n", text.find(marker) + len(marker)) + 1
    # End at next ════ separator or end of file
    end = text.find("=" * 20, content_start)
    caption = text[content_start:end if end != -1 else None].strip()

    # Facebook captions: remove the ━━━ header lines (already extracted above)
    caption = caption.replace(
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", ""
    ).strip()

    caption = caption.replace("[YOUR LINK]", "https://wa.me/447459920895")

    return caption


# ── Facebook API calls ────────────────────────────────────────────────────────
def _check_credentials():
    if not FB_PAGE_ID or not FB_PAGE_TOKEN:
        print(
            "\n[ERROR] Facebook credentials not configured.\n"
            "Add to configs/.env:\n"
            "  FB_PAGE_ID=your_page_id\n"
            "  FB_PAGE_ACCESS_TOKEN=your_long_lived_page_token\n"
            "\nSee: https://developers.facebook.com/docs/pages/access-tokens"
        )
        sys.exit(1)


def post_video(video_path: Path, caption: str, scheduled_unix: int = None,
               dry_run: bool = False) -> dict:
    """Upload a video to the Facebook Page."""
    if dry_run:
        sched = datetime.utcfromtimestamp(scheduled_unix).strftime("%Y-%m-%d %H:%M UTC") \
                if scheduled_unix else "now"
        print(f"  [DRY-RUN] Would post: {video_path.name}")
        print(f"            Scheduled:  {sched}")
        safe_caption = caption[:80].encode("ascii", "replace").decode("ascii")
        print(f"            Caption:    {safe_caption}...")
        return {"id": "dry-run"}

    url     = f"{GRAPH_API}/{FB_PAGE_ID}/videos"
    payload = {
        "access_token": FB_PAGE_TOKEN,
        "description":  caption,
    }
    if scheduled_unix:
        payload["published"]       = "false"
        payload["scheduled_publish_time"] = str(scheduled_unix)

    with open(video_path, "rb") as f:
        resp = requests.post(url, data=payload, files={"source": f}, timeout=120)

    resp.raise_for_status()
    return resp.json()


def post_photo(photo_path: Path, caption: str, dry_run: bool = False) -> dict:
    """Fallback: post a PNG photo if video encoding was skipped."""
    if dry_run:
        print(f"  [DRY-RUN] Would post photo: {photo_path.name}")
        print(f"            Caption: {caption[:80]}...")
        return {"id": "dry-run"}

    url     = f"{GRAPH_API}/{FB_PAGE_ID}/photos"
    payload = {"access_token": FB_PAGE_TOKEN, "caption": caption}
    with open(photo_path, "rb") as f:
        resp = requests.post(url, data=payload, files={"source": f}, timeout=60)
    resp.raise_for_status()
    return resp.json()


def verify_token() -> bool:
    """Quick check that the page token is valid."""
    resp = requests.get(
        f"{GRAPH_API}/{FB_PAGE_ID}",
        params={"access_token": FB_PAGE_TOKEN, "fields": "name,id"},
        timeout=10,
    )
    if resp.ok:
        data = resp.json()
        print(f"[OK] Connected to Facebook Page: {data.get('name')} (id={data.get('id')})")
        return True
    print(f"[ERROR] Token verification failed: {resp.text}")
    return False


# ── Schedule calculator ───────────────────────────────────────────────────────
def schedule_unix_for_day(day_name: str) -> int:
    """Return Unix timestamp for the next occurrence of day_name at POST_HOUR_UTC."""
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    today     = datetime.utcnow()
    target_wd = days.index(day_name)
    delta     = (target_wd - today.weekday()) % 7
    target_dt = (today + timedelta(days=delta)).replace(
        hour=POST_HOUR_UTC, minute=0, second=0, microsecond=0
    )
    # If it's already past that time today, push to next week
    if delta == 0 and today.hour >= POST_HOUR_UTC:
        target_dt += timedelta(weeks=1)
    return int(target_dt.timestamp())


# ── Main ──────────────────────────────────────────────────────────────────────
def post_day(day: str, schedule: bool = True, dry_run: bool = False, delete_after: bool = False):
    slug, _ = DAY_MAP[day]
    caption = get_caption(day)

    # Prefer MP4, fall back to PNG
    mp4  = CARDS_DIR / f"{slug}.mp4"
    png  = CARDS_DIR / f"{slug}.png"
    path = mp4 if mp4.exists() else png

    if not path.exists():
        print(f"  [SKIP] No card found for {day}: {path}")
        return

    sched_ts = schedule_unix_for_day(day) if schedule else None

    print(f"  Posting {day} ({path.name}) ...")
    try:
        if path.suffix == ".mp4":
            result = post_video(path, caption, scheduled_unix=sched_ts, dry_run=dry_run)
        else:
            result = post_photo(path, caption, dry_run=dry_run)
        print(f"  [OK] id={result.get('id')}")
        # Delete MP4 after successful post — PNG kept as lightweight archive
        if delete_after and not dry_run and path.suffix == ".mp4" and path.exists():
            path.unlink()
            print(f"  [CLEANUP] Deleted {path.name} (saved to socials)")
    except requests.HTTPError as e:
        print(f"  [FAILED] {e.response.status_code}: {e.response.text}")


def main():
    parser = argparse.ArgumentParser(description="Post weekly property cards to Facebook Page")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--day",  choices=list(DAY_MAP.keys()), help="Post a single day's card now")
    grp.add_argument("--all",  action="store_true",           help="Schedule all 7 cards for the week")
    parser.add_argument("--now",          action="store_true", help="Post immediately (no scheduling)")
    parser.add_argument("--dry-run",      action="store_true", help="Preview posts without actually posting")
    parser.add_argument("--delete-after", action="store_true", help="Delete MP4 after successful post (keeps PNG)")
    args = parser.parse_args()

    if not args.dry_run:
        _check_credentials()
        if not verify_token():
            sys.exit(1)

    schedule = not args.now

    if args.day:
        post_day(args.day, schedule=schedule, dry_run=args.dry_run, delete_after=args.delete_after)
    else:
        print(f"Scheduling all 7 posts for week of {date.today().strftime('%d %B %Y')}...\n")
        for day in DAY_MAP:
            post_day(day, schedule=schedule, dry_run=args.dry_run, delete_after=args.delete_after)
            time.sleep(1)   # be polite to the API

    print("\nDone.")


if __name__ == "__main__":
    main()
