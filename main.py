#!/usr/bin/env python3
"""
NHK BSP4K Movie Broadcast Checker
==================================
Sources
  1. Monthly calendar blog posts on nhk.jp — full-month coverage, up to ~2 months ahead.
     Each post is published ~2–4 weeks before the month and lists every BSP4K broadcast
     clearly labeled "NHK BSP4K".
     Blog index: https://www.nhk.jp/g/ts/K8649395M1/blog/bl/pLAv8dgRAB/

  2. Series /schedule page on web.nhk — covers ~1 week ahead with exact program names.
     Used to override inferred program names from the blog and catch the very near term.
     https://www.web.nhk/tv/pl/series-tep-K8649395M1/schedule

  3. JustWatch Japan GraphQL API — subscription streaming availability.

Output: nhk_bsp4k_movies.csv

Setup:
    uv add playwright simple-justwatch-python-api
    uv run playwright install chromium

Run:
    uv run main.py
"""

import asyncio
import csv
import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from playwright.async_api import Page, async_playwright
from simplejustwatchapi.justwatch import search as jw_search

# ── Configuration ──────────────────────────────────────────────────────────────

OUTPUT_FILE = Path("nhk_bsp4k_movies.csv")
JSON_FILE = Path("data.json")
HEADLESS = True

# Monthly blog index (all calendar posts for プレミアムシネマ / シネマ4K)
NHK_BLOG_INDEX_URL = "https://www.nhk.jp/g/ts/K8649395M1/blog/bl/pLAv8dgRAB/"

# Series pages on web.nhk
NHK_SERIES_BASE_URL     = "https://www.web.nhk/tv/pl/series-tep-K8649395M1/"
NHK_SERIES_SCHEDULE_URL = "https://www.web.nhk/tv/pl/series-tep-K8649395M1/schedule"
BSP4K_CHANNEL_CODE = "s5"   # present in href as  …schedule-tep-s5-…

# Text label used for BSP4K entries in the blog calendar posts
BSP4K_BLOG_LABEL = "NHK BSP4K"

# Standard subscription services.
# Matching uses exact equality against offer.package.technical_name to avoid
# false positives from add-on channels (e.g. "unexthbomax" must NOT match U-NEXT).
STREAMING_SERVICES: dict[str, set[str]] = {
    "Netflix":              {"netflix"},
    "Amazon Prime Video":   {"amazonprime", "amazonprimevideowithads"},
    "Disney+":              {"disneyplus"},
    "Hulu":                 {"hulu"},
    "U-NEXT":               {"unext"},
    "Apple TV":             {"appletvplus"},
    "ABEMA":                {"abema"},
    "WOWOW":                {"wowow"},
    "FOD":                  {"fod", "fod_premium"},
    "dTV":                  {"dtv"},
}

# Add-on channels that require an extra subscription fee on top of a base service.
# Shown as separate CSV columns so the reader knows additional cost is involved.
ADDON_CHANNELS: dict[str, set[str]] = {
    "HBO Max (U-NEXT+fee)":      {"unexthbomax"},
    "Apple TV (Amazon+fee)":    {"amazonappletvplus"},
    "KADOKAWA ch (Amazon+fee)":  {"amazonccbkadokawa"},
}

# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class Movie:
    broadcast_date: str
    broadcast_time: str
    program_name: str   # e.g. "シネマ4K" or "プレミアムシネマ4K"
    title: str
    page_url: str = ""
    streaming: dict[str, bool] = field(default_factory=dict)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _ja_time_to_hhmm(ampm: str, h: int, mn: int) -> str:
    """Convert 午前/午後 + H:MM to zero-padded 24-hour HH:MM string."""
    if ampm == "午後" and h != 12:
        h += 12
    elif ampm == "午前" and h == 12:
        h = 0
    return f"{h:02d}:{mn:02d}"


def _infer_program_name(title: str) -> str:
    """Guess program slot from title.  Titles with '4K' restoration markers → シネマ4K."""
    t = title.lower()
    if any(s in t for s in ("4kデジタル修復版", "4k修復版", "4k版")):
        return "シネマ4K"
    return "プレミアムシネマ4K"

# ── Source 1: monthly blog calendar ────────────────────────────────────────────

async def _goto_with_retry(page: Page, url: str, retries: int = 3, timeout: int = 30_000) -> None:
    """Navigate to url, retrying on network errors with exponential back-off."""
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            return
        except Exception as exc:
            if attempt == retries:
                raise
            wait_sec = 2 ** attempt
            print(f"  goto failed (attempt {attempt}/{retries}): {exc!r} — retrying in {wait_sec}s …", file=sys.stderr)
            await asyncio.sleep(wait_sec)


async def fetch_blog_calendar_urls(page: Page) -> list[tuple[str, int]]:
    """Scrape the blog index and return (post_url, year) for monthly calendar posts."""
    await _goto_with_retry(page, NHK_BLOG_INDEX_URL, timeout=30_000)
    await page.wait_for_timeout(1_500)

    posts: list[dict] = await page.evaluate(
        """() => [...document.querySelectorAll("a[href*='/bp/']")]
                .map(a => ({text: a.textContent.trim(), href: a.href}))"""
    )

    results = []
    for post in posts:
        m = re.search(r"(\d{4})年\d+月の映画", post["text"])
        if m:
            results.append((post["href"], int(m.group(1))))
    return results


async def scrape_blog_calendar(page: Page, url: str, year: int) -> list[Movie]:
    """Extract NHK BSP4K movies from one monthly blog post."""
    await _goto_with_retry(page, url, timeout=30_000)
    await page.wait_for_timeout(1_500)
    body = await page.inner_text("body")

    # Walk every node in document order.  Emit a "bsp4k" event for each text node
    # line that contains the BSP4K label, and a "detail" event for each <a> whose
    # text contains "詳しく見る".  Then pair consecutive bsp4k→detail events.
    raw_pairs: list[dict] = await page.evaluate(
        """() => {
            const BSP4K   = "NHK BSP4K";
            const DETAIL  = "詳しく見る";
            const events  = [];

            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ALL);
            let node;
            while ((node = walker.nextNode())) {
                if (node.nodeType === Node.TEXT_NODE) {
                    for (const line of node.textContent.split(/\\n/)) {
                        if (line.includes(BSP4K))
                            events.push({ type: "bsp4k", text: line.trim() });
                    }
                } else if (node.nodeType === Node.ELEMENT_NODE &&
                           node.tagName === "A" &&
                           node.textContent.trim().includes(DETAIL)) {
                    events.push({ type: "detail", href: node.href });
                }
            }

            const pairs = [];
            let pending = null;
            for (const ev of events) {
                if (ev.type === "bsp4k") {
                    pending = ev.text;          // remember the most-recent BSP4K line
                } else if (ev.type === "detail" && pending) {
                    pairs.push({ bsp4kLine: pending, href: ev.href });
                    pending = null;
                }
            }
            return pairs;
        }"""
    )

    # Follow each "詳しく見る" link and resolve to an /ep/ URL.
    # We have already captured `body` so navigating away is fine.
    href_map: dict[str, str] = {}
    for pair in raw_pairs:
        dest = pair["href"]
        if "/ep/" in dest:
            ep_url = dest
        else:
            try:
                await _goto_with_retry(page, dest, timeout=20_000)
                await page.wait_for_timeout(800)
                ep_url = await page.evaluate(
                    """() => {
                        const a = document.querySelector('a[href*="/ep/"]');
                        return a ? a.href : (location.href.includes("/ep/") ? location.href : "");
                    }"""
                )
            except Exception as exc:
                print(f"  [warn] could not follow detail link {dest}: {exc}", file=sys.stderr)
                ep_url = ""

        if ep_url:
            key = unicodedata.normalize("NFKC", pair["bsp4kLine"]).strip()
            href_map[key] = ep_url

    return _parse_blog_text(body, year, href_map)


def _parse_blog_text(text: str, year: int, href_map: dict[str, str] | None = None) -> list[Movie]:
    """Parse the flat text of a monthly calendar blog post.

    Entries look like:
        ■「ワンス・アポン・ア・タイム・イン・ハリウッド」
        NHK BSP4K 3月7日(土)午後9:00～
    """
    href_map = href_map or {}
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    movies: list[Movie] = []

    for i, line in enumerate(lines):
        if BSP4K_BLOG_LABEL not in line:
            continue

        # Find movie title in the preceding lines (「…」 or ■ prefix)
        title = ""
        for j in range(i - 1, max(-1, i - 5), -1):
            prev = lines[j]
            m = re.search(r"「([^」]+)」", prev)
            if m:
                title = m.group(1).strip()
                break
            if prev.startswith("■"):
                title = prev.lstrip("■").strip()
                break

        if not title:
            continue

        # Normalize fullwidth → halfwidth (e.g. ４Ｋ → 4K)
        title = unicodedata.normalize("NFKC", title).strip()

        # Parse date
        date_m = re.search(r"(\d+)月(\d+)日", line)
        if not date_m:
            continue
        month, day = int(date_m.group(1)), int(date_m.group(2))

        # Parse time
        time_m = re.search(r"(午前|午後)(\d+):(\d+)", line)
        broadcast_time = (
            _ja_time_to_hhmm(time_m.group(1), int(time_m.group(2)), int(time_m.group(3)))
            if time_m else ""
        )

        try:
            bd = date(year, month, day)
        except ValueError:
            continue

        page_url = href_map.get(unicodedata.normalize("NFKC", line).strip(), "")
        movies.append(Movie(
            broadcast_date=bd.isoformat(),
            broadcast_time=broadcast_time,
            program_name=_infer_program_name(title),
            title=title,
            page_url=page_url,
        ))

    return movies

# ── Source 2: series /schedule page ────────────────────────────────────────────


async def _collect_ep_url_map(page: Page) -> dict[str, str]:
    """Query all /ep/ links on the current page and return title → URL.

    The /ep/ link itself may be a small button (e.g. "詳細を見る") whose own
    textContent does not contain the movie title.  We therefore walk up the DOM
    from each link to find the nearest ancestor element whose text includes a
    「Title」 pattern.
    """
    raw: list[dict] = await page.evaluate(
        """() => {
            const seen = new Set();
            return [...document.querySelectorAll('a[href*="/ep/"]')]
                .filter(a => {
                    if (seen.has(a.href)) return false;
                    seen.add(a.href);
                    return true;
                })
                .map(a => {
                    // Walk up at most 6 ancestor levels to find 「Title」 text.
                    let el = a;
                    for (let i = 0; i < 6; i++) {
                        const m = el.textContent.match(/「([^」]+)」/);
                        if (m) return { title: m[1].trim(), href: a.href };
                        if (!el.parentElement) break;
                        el = el.parentElement;
                    }
                    return null;
                })
                .filter(Boolean);
        }"""
    )
    ep_map: dict[str, str] = {}
    for item in raw:
        title = unicodedata.normalize("NFKC", item["title"]).strip()
        if title and title not in ep_map:
            ep_map[title] = item["href"]
    return ep_map


async def scrape_series_schedule(page: Page) -> list[Movie]:
    """Scrape the series /schedule page for BSP4K items (href contains -s5-)."""
    await _goto_with_retry(page, NHK_SERIES_SCHEDULE_URL, timeout=25_000)
    await page.wait_for_timeout(1_500)

    items: list[dict] = await page.evaluate(
        f"""(code) => {{
            const results = [];
            document.querySelectorAll("li").forEach(li => {{
                const a = li.querySelector("a[href]");
                if (!a || !new RegExp("-" + code + "-").test(a.href)) return;
                const titleEl = li.querySelector(".program_title");
                results.push({{
                    text: (titleEl || li).textContent.trim(),
                    href: a.href
                }});
            }});
            return results;
        }}""",
        BSP4K_CHANNEL_CODE,
    )

    movies: list[Movie] = []
    for item in items:
        text = item["text"]
        href = item["href"]

        # Broadcast date from href: …-s5-130-YYYYMMDD/…
        date_m = re.search(r"-s5-\d+-(\d{8})", href)
        if not date_m:
            continue
        ds = date_m.group(1)
        bd = date(int(ds[:4]), int(ds[4:6]), int(ds[6:8]))

        # Normalize and parse
        text_n = unicodedata.normalize("NFKC", text)

        time_m = re.search(r"(午前|午後)(\d+):(\d+)", text_n)
        broadcast_time = (
            _ja_time_to_hhmm(time_m.group(1), int(time_m.group(2)), int(time_m.group(3)))
            if time_m else ""
        )

        prog_m = re.match(r"(シネマ4K|プレミアムシネマ4K|プレミアムシネマ|シネマ)", text_n)
        program_name = prog_m.group(1) if prog_m else ""

        title_m = re.search(r"「([^」]+)」", text_n)
        if not title_m:
            continue
        title = title_m.group(1).strip()

        # Visit the per-broadcast schedule page to get the episode /ep/ URL.
        ep_url = ""
        try:
            await _goto_with_retry(page, href, timeout=20_000)
            await page.wait_for_timeout(500)
            ep_url = await page.evaluate(
                """() => { const a = document.querySelector('a[href*="/ep/"]'); return a ? a.href : ""; }"""
            )
        except Exception as exc:
            print(f"  [warn] could not fetch schedule page {href}: {exc}", file=sys.stderr)

        movies.append(Movie(
            broadcast_date=bd.isoformat(),
            broadcast_time=broadcast_time,
            program_name=program_name,
            title=title,
            page_url=ep_url,
        ))

    return movies

# ── Source 3: series episode list ──────────────────────────────────────────────

async def fetch_episode_url_map(page: Page) -> dict[str, str]:
    """Scrape the series main page for all /ep/ links and return title → URL."""
    await _goto_with_retry(page, NHK_SERIES_BASE_URL, timeout=30_000)
    await page.wait_for_timeout(1_500)
    return await _collect_ep_url_map(page)


# ── JustWatch ──────────────────────────────────────────────────────────────────

def _check_justwatch_jp(movie_title: str) -> dict[str, bool]:
    """Query JustWatch Japan for FLATRATE offers.

    Returns a dict covering both STREAMING_SERVICES (standard subscriptions)
    and ADDON_CHANNELS (extra-fee add-ons), keyed by their column names.
    Matching is by exact technical_name to avoid cross-contamination between
    e.g. 'unext' (standard) and 'unexthbomax' (HBO Max add-on).
    """
    all_columns = {**STREAMING_SERVICES, **ADDON_CHANNELS}
    availability = {name: False for name in all_columns}

    for lang in ("ja", "en"):
        try:
            results = jw_search(movie_title, "JP", lang, count=5, best_only=False)
        except Exception as exc:
            print(f"JustWatch error ({lang}): {exc}", file=sys.stderr)
            continue
        if not results:
            continue

        entry = next((r for r in results if r.object_type == "MOVIE"), results[0])
        for offer in entry.offers:
            if offer.monetization_type != "FLATRATE":
                continue
            tech = (offer.package.technical_name or "").lower()
            for col, tech_names in all_columns.items():
                if not availability[col] and tech in tech_names:
                    availability[col] = True
        break   # success — no need to retry in English

    return availability


async def check_justwatch_jp(title: str) -> dict[str, bool]:
    """Async wrapper — runs the synchronous JustWatch call in a thread."""
    return await asyncio.to_thread(_check_justwatch_jp, title)

# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            args=["--disable-http2"],
        )
        ctx = await browser.new_context(
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()

        # ── 1a. Monthly blog calendars ────────────────────────────────────────
        print("Fetching monthly calendar posts …")
        cal_urls = await fetch_blog_calendar_urls(page)
        if cal_urls:
            print(f"  Found {len(cal_urls)} calendar(s)")
        else:
            print("  No calendar posts found — blog index may have changed.", file=sys.stderr)

        blog_movies: list[Movie] = []
        for url, year in cal_urls:
            m_text = re.search(r"(\d+月の映画)", url) or re.search(r"bp/(\w+)", url)
            label = m_text.group(1) if m_text else url
            print(f"  Fetching {label} ({year}) … ", end="", flush=True)
            found = await scrape_blog_calendar(page, url, year)
            print(f"{len(found)} BSP4K movie(s)")
            blog_movies.extend(found)

        # ── 1b. /schedule page (near-term, exact program names) ───────────────
        print("\nFetching series /schedule page …")
        schedule_items = await scrape_series_schedule(page)
        print(f"  {len(schedule_items)} BSP4K item(s)")

        # ── 1c. Series episode list (episode-level page URLs) ─────────────────
        print("\nFetching series episode list …")
        episode_url_map = await fetch_episode_url_map(page)
        print(f"  {len(episode_url_map)} episode URL(s)")

        await browser.close()

    # ── Merge: use (date, title) as key; /schedule overrides blog ────────────
    merged: dict[tuple[str, str], Movie] = {}
    for m in blog_movies:
        merged[(m.broadcast_date, m.title)] = m
    for m in schedule_items:
        merged[(m.broadcast_date, m.title)] = m   # /schedule wins

    all_movies = sorted(merged.values(), key=lambda m: (m.broadcast_date, m.broadcast_time))

    # Fill in episode page URLs (prefer already-set /ep/ URLs from the schedule scraper).
    linked = 0
    for m in all_movies:
        if not m.page_url and m.title in episode_url_map:
            m.page_url = episode_url_map[m.title]
        if m.page_url:
            linked += 1
    print(f"  Episode URLs linked: {linked} / {len(all_movies)}")
    if not any(m.page_url for m in all_movies):
        sample_titles = [m.title for m in all_movies[:3]]
        sample_ep    = list(episode_url_map.items())[:3]
        print(f"  [debug] sample movie titles : {sample_titles}", file=sys.stderr)
        print(f"  [debug] sample ep_map keys  : {[k for k,_ in sample_ep]}", file=sys.stderr)

    if not all_movies:
        print(
            "\nNo BSP4K movies found.\n"
            "Possible causes:\n"
            "  • No monthly calendar post published yet for upcoming months\n"
            "  • Blog index URL changed — check:\n"
            f"    {NHK_BLOG_INDEX_URL}",
            file=sys.stderr,
        )
        return

    # Deduplicate titles for JustWatch (same movie may air multiple times)
    seen: set[str] = set()
    unique: list[Movie] = []
    for m in all_movies:
        if m.title not in seen:
            seen.add(m.title)
            unique.append(m)

    print(f"\nBroadcasts: {len(all_movies)}  |  Unique titles: {len(unique)}")

    # ── 2. JustWatch streaming check ─────────────────────────────────────────
    print("\nChecking JustWatch Japan …")
    streaming_cache: dict[str, dict[str, bool]] = {}

    BATCH = 4
    for batch_start in range(0, len(unique), BATCH):
        batch = unique[batch_start : batch_start + BATCH]
        results = await asyncio.gather(
            *[check_justwatch_jp(m.title) for m in batch],
            return_exceptions=True,
        )
        for movie, result in zip(batch, results):
            if isinstance(result, Exception):
                print(f"  [!] {movie.title}: {result}", file=sys.stderr)
                streaming_cache[movie.title] = {s: False for s in STREAMING_SERVICES}
            else:
                streaming_cache[movie.title] = result
                hit = [s for s, v in result.items() if v]
                print(f"  {movie.title}")
                print(f"      → {', '.join(hit) if hit else '(not on streaming)'}")

    for m in all_movies:
        m.streaming = streaming_cache.get(m.title, {s: False for s in STREAMING_SERVICES})

    # ── 3. Write CSV ──────────────────────────────────────────────────────────
    fieldnames = [
        "broadcast_date", "broadcast_time", "program_name", "title",
        *STREAMING_SERVICES.keys(),
        *ADDON_CHANNELS.keys(),
    ]
    with OUTPUT_FILE.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in all_movies:
            row: dict = {
                "broadcast_date": m.broadcast_date,
                "broadcast_time": m.broadcast_time,
                "program_name":   m.program_name,
                "title":          m.title,
            }
            row.update(m.streaming)
            writer.writerow(row)

    print(f"\nSaved {len(all_movies)} row(s) → {OUTPUT_FILE.resolve()}")

    # ── 4. Write JSON for the web frontend ───────────────────────────────────
    json_payload = {
        "updated": date.today().isoformat(),
        "services": list(STREAMING_SERVICES.keys()),
        "addon_channels": list(ADDON_CHANNELS.keys()),
        "movies": [
            {
                "broadcast_date": m.broadcast_date,
                "broadcast_time": m.broadcast_time,
                "program_name": m.program_name,
                "title": m.title,
                "page_url": m.page_url,
                "streaming": m.streaming,
            }
            for m in all_movies
        ],
    }
    with JSON_FILE.open("w", encoding="utf-8") as f:
        json.dump(json_payload, f, ensure_ascii=False, indent=2)
    print(f"Saved JSON      → {JSON_FILE.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
