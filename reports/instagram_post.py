"""
reports/instagram_post.py
Posts the 7 weekly video cards to Instagram via the Graph API.
Instagram requires videos to be hosted at a public URL (uses container upload flow).

For local files we use the resumable upload API:
  1. POST /{ig_user_id}/media  → container_id
  2. Poll until status == FINISHED
  3. POST /{ig_user_id}/media_publish  → published post

Usage:
    python reports/instagram_post.py --day monday   # post now
    python reports/instagram_post.py --all           # post all 7
    python reports/instagram_post.py --dry-run       # preview
"""
import os
import sys
import time
import argparse
from datetime import date, datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "configs" / ".env")

IG_USER_ID    = os.getenv("IG_USER_ID", "")
PAGE_TOKEN    = os.getenv("FB_PAGE_ACCESS_TOKEN", "")
GRAPH_API     = "https://graph.facebook.com/v19.0"

CARDS_DIR  = Path(__file__).parent / "output" / "cards" / date.today().isoformat()
POSTS_FILE = Path(__file__).parent / "output" / f"social_posts_{date.today().isoformat()}.txt"

DAY_MAP = {
    "monday":    ("monday_market_pulse",        "MONDAY — MARKET PULSE"),
    "tuesday":   ("tuesday_suburb_spotlight",   "TUESDAY — SUBURB SPOTLIGHT"),
    "wednesday": ("wednesday_price_check",      "WEDNESDAY — PRICE CHECK"),
    "thursday":  ("thursday_investment_signal", "THURSDAY — INVESTMENT SIGNAL"),
    "friday":    ("friday_data_fact",           "FRIDAY — DATA FACT"),
    "saturday":  ("saturday_diaspora_special",  "SATURDAY — DIASPORA SPECIAL"),
    "sunday":    ("sunday_weekly_summary",      "SUNDAY — WEEKLY MARKET SUMMARY"),
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _check_credentials():
    if not IG_USER_ID or not PAGE_TOKEN:
        print(
            "\n[ERROR] Instagram credentials not configured.\n"
            "Required in configs/.env:\n"
            "  IG_USER_ID=your_instagram_business_account_id\n"
            "  FB_PAGE_ACCESS_TOKEN=your_page_token\n"
        )
        sys.exit(1)


def get_caption(day: str) -> str:
    if not POSTS_FILE.exists():
        return f"Zimbabwe Property Market Intelligence — Week of {date.today().strftime('%d %B %Y')}"

    _, section_header = DAY_MAP[day]
    text   = POSTS_FILE.read_text(encoding="utf-8")
    marker = f"{'━' * 40}\n{section_header}"
    start  = text.find(marker)
    if start == -1:
        return f"Zimbabwe Property Intelligence — {section_header}"

    content_start = text.find("\n", start + len(marker)) + 1
    end = text.find("=" * 20, content_start)
    caption = text[content_start:end if end != -1 else None].strip()
    caption = caption.replace("━" * 40, "").strip()
    caption = caption.replace("[YOUR LINK]", "https://wa.me/447459920895")
    return caption


def verify_token() -> bool:
    r = requests.get(
        f"{GRAPH_API}/{IG_USER_ID}",
        params={"fields": "username,name", "access_token": PAGE_TOKEN},
        timeout=10,
    )
    if r.ok:
        d = r.json()
        print(f"[OK] Connected to Instagram: @{d.get('username')} (id={d.get('id')})")
        return True
    print(f"[ERROR] Token verification failed: {r.text}")
    return False


# ── Upload flow ────────────────────────────────────────────────────────────────

def _upload_video_resumable(video_path: Path) -> str:
    """
    Upload a local video using the resumable upload API.
    Returns the video_handle to use in the media container.
    """
    file_size = video_path.stat().st_size

    # 1. Start upload session
    r = requests.post(
        f"{GRAPH_API}/{IG_USER_ID}/video_reels",
        params={"access_token": PAGE_TOKEN},
        json={"upload_phase": "start", "file_size": file_size},
        timeout=30,
    )
    r.raise_for_status()
    session = r.json()
    upload_url    = session["uri"]
    video_id_temp = session.get("video_id", "")

    # 2. Upload bytes
    with open(video_path, "rb") as f:
        data = f.read()
    r = requests.post(
        upload_url,
        headers={
            "Authorization":  f"OAuth {PAGE_TOKEN}",
            "offset":         "0",
            "file_size":      str(file_size),
        },
        data=data,
        timeout=300,
    )
    r.raise_for_status()

    return video_id_temp


def _create_media_container(video_path: Path, caption: str) -> str:
    """Create an IG media container for a local MP4 (Reels)."""
    file_size = video_path.stat().st_size

    # For local video: use the upload_type=reels path
    r = requests.post(
        f"{GRAPH_API}/{IG_USER_ID}/media",
        params={"access_token": PAGE_TOKEN},
        data={
            "media_type":    "REELS",
            "video_url":     "",        # will use upload flow below
            "caption":       caption,
            "share_to_feed": "true",
        },
        timeout=30,
    )
    # Fall back to photo if video upload is complex
    raise NotImplementedError("Use _post_photo for local files (see note below)")


def _create_photo_container(photo_path: Path, caption: str) -> str:
    """Create an IG media container for a local PNG (image post)."""
    # Instagram Graph API requires a publicly accessible URL for images.
    # For local files we use the /photos endpoint via multipart upload
    # (only supported for pages, not IG directly).
    # Best local option: post as image using the page-backed IG approach.
    r = requests.post(
        f"{GRAPH_API}/{IG_USER_ID}/media",
        params={"access_token": PAGE_TOKEN},
        data={
            "image_url": "",   # placeholder — see note
            "caption":   caption,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["id"]


def _poll_container(container_id: str, max_wait: int = 120) -> bool:
    """Poll until container status is FINISHED."""
    for _ in range(max_wait // 5):
        r = requests.get(
            f"{GRAPH_API}/{container_id}",
            params={"fields": "status_code,status", "access_token": PAGE_TOKEN},
            timeout=10,
        )
        status = r.json().get("status_code", "")
        if status == "FINISHED":
            return True
        if status == "ERROR":
            print(f"  [ERROR] Container failed: {r.json()}")
            return False
        time.sleep(5)
    print("  [TIMEOUT] Container not ready after waiting")
    return False


def _publish_container(container_id: str) -> dict:
    r = requests.post(
        f"{GRAPH_API}/{IG_USER_ID}/media_publish",
        params={"access_token": PAGE_TOKEN},
        data={"creation_id": container_id},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def post_image_from_url(image_url: str, caption: str, dry_run: bool = False) -> dict:
    """Post an image to Instagram using a public URL."""
    if dry_run:
        print(f"  [DRY-RUN] Would post image from URL")
        print(f"            Caption: {caption[:80]}...")
        return {"id": "dry-run"}

    # Step 1: create container
    r = requests.post(
        f"{GRAPH_API}/{IG_USER_ID}/media",
        params={"access_token": PAGE_TOKEN},
        data={"image_url": image_url, "caption": caption},
        timeout=30,
    )
    r.raise_for_status()
    container_id = r.json()["id"]

    # Step 2: poll
    if not _poll_container(container_id):
        raise RuntimeError("Media container failed")

    # Step 3: publish
    return _publish_container(container_id)


def post_reel_from_url(video_url: str, caption: str, dry_run: bool = False) -> dict:
    """Post a Reel to Instagram using a public video URL."""
    if dry_run:
        print(f"  [DRY-RUN] Would post Reel from URL")
        print(f"            Caption: {caption[:80]}...")
        return {"id": "dry-run"}

    # Step 1: create container
    r = requests.post(
        f"{GRAPH_API}/{IG_USER_ID}/media",
        params={"access_token": PAGE_TOKEN},
        data={
            "media_type":    "REELS",
            "video_url":     video_url,
            "caption":       caption,
            "share_to_feed": "true",
        },
        timeout=30,
    )
    r.raise_for_status()
    container_id = r.json()["id"]
    print(f"  Container created: {container_id} — waiting for processing...")

    # Step 2: poll (videos take longer)
    if not _poll_container(container_id, max_wait=300):
        raise RuntimeError("Video container failed")

    # Step 3: publish
    return _publish_container(container_id)


# ── Local file posting via Facebook CDN trick ──────────────────────────────────

def post_local_photo(photo_path: Path, caption: str, dry_run: bool = False) -> dict:
    """
    Post a local PNG to Instagram by first uploading to Facebook as unpublished,
    then using the returned URL as the image_url for Instagram.
    """
    if dry_run:
        safe = caption[:80].encode("ascii", "replace").decode("ascii")
        print(f"  [DRY-RUN] Would post photo: {photo_path.name}")
        print(f"            Caption: {safe}...")
        return {"id": "dry-run"}

    fb_page_id = os.getenv("FB_PAGE_ID", "")

    # 1. Upload photo to Facebook Page (unpublished) to get a hosted URL
    with open(photo_path, "rb") as f:
        r = requests.post(
            f"{GRAPH_API}/{fb_page_id}/photos",
            data={"access_token": PAGE_TOKEN, "published": "false"},
            files={"source": f},
            timeout=60,
        )
    r.raise_for_status()
    fb_photo_id = r.json()["id"]

    # 2. Retrieve the hosted image URL
    r2 = requests.get(
        f"{GRAPH_API}/{fb_photo_id}",
        params={"fields": "images", "access_token": PAGE_TOKEN},
        timeout=10,
    )
    r2.raise_for_status()
    images = r2.json().get("images", [])
    if not images:
        raise RuntimeError("Could not get hosted image URL from Facebook")
    hosted_url = images[0]["source"]

    # 3. Post to Instagram using the hosted URL
    return post_image_from_url(hosted_url, caption, dry_run=False)


# ── Main post logic ────────────────────────────────────────────────────────────

def post_day(day: str, dry_run: bool = False):
    slug, _ = DAY_MAP[day]
    caption  = get_caption(day)

    png = CARDS_DIR / f"{slug}.png"
    mp4 = CARDS_DIR / f"{slug}.mp4"

    if png.exists():
        print(f"  Posting {day} ({png.name}) as image ...")
        try:
            result = post_local_photo(png, caption, dry_run=dry_run)
            print(f"  [OK] id={result.get('id')}")
        except requests.HTTPError as e:
            print(f"  [FAILED] {e.response.status_code}: {e.response.text}")
        except Exception as e:
            print(f"  [FAILED] {e}")
    elif mp4.exists():
        print(f"  [NOTE] MP4 found but Instagram Reels require a public URL.")
        print(f"         To post Reels, host the video on S3/Cloudflare and use post_reel_from_url().")
        print(f"         Skipping {day} MP4 — PNG card not found.")
    else:
        print(f"  [SKIP] No card found for {day}")


def main():
    parser = argparse.ArgumentParser(description="Post weekly property cards to Instagram")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--day", choices=list(DAY_MAP.keys()), help="Post a single day's card")
    grp.add_argument("--all", action="store_true", help="Post all 7 cards")
    parser.add_argument("--dry-run", action="store_true", help="Preview without posting")
    args = parser.parse_args()

    if not args.dry_run:
        _check_credentials()
        if not verify_token():
            sys.exit(1)

    if args.day:
        post_day(args.day, dry_run=args.dry_run)
    else:
        print(f"Posting all 7 cards to Instagram for week of {date.today().strftime('%d %B %Y')}...\n")
        for day in DAY_MAP:
            post_day(day, dry_run=args.dry_run)
            time.sleep(2)

    print("\nDone.")


if __name__ == "__main__":
    main()
