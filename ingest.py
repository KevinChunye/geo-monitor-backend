# ingest.py
"""
Ingestion layer for the Geopolitical/Policy Monitor (Copper MVP).

Sources (MVP):
- GDELT 2.1 DOC API (discovery)
- Mining.com RSS (industry)
- U.S. Treasury press releases page (official HTML list scrape)
- OFAC recent actions page (official HTML list scrape; may timeout — best-effort)
- U.S. State Dept RSS feeds (you must fill FEEDS with real RSS URLs)

Design goals:
- NEVER crash the whole ingestion if one source fails.
- Return a per-source report via run_all.last_report.
- Keep event schema compatible with the frontend.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Tuple, Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparse


# ----------------------------
# Configuration
# ----------------------------

COPPER_KW = [
    "copper", "codelco", "smelter", "smelting", "refining", "refinery",
    "concentrate", "cathode", "mine", "mining", "tcrc", "cu "
]

RISK_KW = {
    "policy": ["regulation", "law", "policy", "ban", "export control", "license", "royalty", "tax", "tariff", "quota"],
    "logistics": ["shipping", "port", "canal", "transport", "delay", "rail", "road", "freight", "blockade", "strait"],
    "labor": ["strike", "protest", "union", "wage", "worker", "labor"],
    "conflict": ["war", "conflict", "attack", "military", "missile", "invasion"],
    "sanctions": ["sanction", "embargo", "restriction", "designation", "sdn"],
}

# Domain -> quality label
ALLOWLIST_QUALITY = {
    "home.treasury.gov": "OFFICIAL",
    "ofac.treasury.gov": "OFFICIAL",
    "state.gov": "OFFICIAL",
    "www.state.gov": "OFFICIAL",
    "mining.com": "INDUSTRY",
    "www.mining.com": "INDUSTRY",
    "reuters.com": "MAJOR_MEDIA",
    "www.reuters.com": "MAJOR_MEDIA",
}

# Drop obvious SEO/finance-blog spam when the source is already classified as OTHER.
DOMAIN_BLOCKLIST = {
    "insidermonkey.com",
    "tickerreport.com",
    "themarketsdaily.com",
}

# NOTE: You MUST fill FEEDS with actual State Dept RSS feed URLs.
# The directory page is not itself a feed.
STATE_DEPT_FEEDS: List[str] = [
    # Example (replace with real feed URLs you choose from https://www.state.gov/rss-feeds/):
    # "https://www.state.gov/feed/",   # <-- This is NOT guaranteed; pick actual feeds from the directory.
]


# ----------------------------
# Helpers
# ----------------------------

def _id(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:18]


def _nowz() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _domain(url: str) -> str:
    m = re.search(r"https?://([^/]+)/", url or "")
    return (m.group(1).lower() if m else "")


def _quality_from_url(url: str) -> str:
    d = _domain(url)
    return ALLOWLIST_QUALITY.get(d, "OTHER")


def _should_drop(url: str) -> bool:
    """Best-effort spam filter. Only drops when the domain is untrusted (OTHER)."""
    d = _domain(url)
    q = _quality_from_url(url)
    return (q == "OTHER") and (d in DOMAIN_BLOCKLIST)


def _clean_summary(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_copper(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in COPPER_KW)


def _risk_types(text: str) -> List[str]:
    t = (text or "").lower()
    out: List[str] = []
    for risk, kws in RISK_KW.items():
        if any(kw in t for kw in kws):
            out.append(risk)
    return out or ["other"]


WHY_IT_MATTERS = {
    "policy": "Potential policy/regulatory change that can affect permitting, exports, taxes, or investment conditions.",
    "logistics": "Potential disruption to transport/ports/shipping that can delay concentrates/cathodes and tighten supply.",
    "labor": "Labor action risk (strike/protest) that can reduce mine/smelter throughput and impact TCRCs/availability.",
    "conflict": "Conflict/security risk that can disrupt operations, infrastructure, or trade routes.",
    "sanctions": "Sanctions/designations risk that can affect counterparties, payments, shipping, and trade compliance.",
    "other": "Potential market-moving development; verify details and linkage to the copper supply chain.",
}


def _why_it_matters(risks: List[str]) -> str:
    r = (risks[0] if risks else "other")
    return WHY_IT_MATTERS.get(r, WHY_IT_MATTERS["other"])


def _severity(text: str, risks: List[str]) -> int:
    """
    Very simple heuristic severity score 0-100.
    """
    t = (text or "").lower()
    score = 25

    # High-impact words
    if any(x in t for x in ["shutdown", "collapse", "ban", "embargo"]):
        score += 40
    elif any(x in t for x in ["strike", "blockade", "attack", "sanction", "halt"]):
        score += 25
    elif any(x in t for x in ["delay", "protest", "tighten", "restriction"]):
        score += 10

    # Risk-type modifiers
    if "sanctions" in risks or "conflict" in risks:
        score += 10
    if "policy" in risks:
        score += 5
    if "logistics" in risks:
        score += 5

    return max(0, min(100, score))


def _mk_event(
    title: str,
    summary: str,
    url: str,
    source_name: str,
    published_at: str,
    location: Optional[Dict[str, Any]] = None,
    force_include: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Build a normalized event matching the frontend schema.
    """
    if _should_drop(url):
        return None

    summary = _clean_summary(summary)
    if not summary and title:
        # fallback so UI never shows blank summaries
        summary = " ".join(title.split()[:28])

    text = f"{title} {summary}"
    if (not force_include) and (not _is_copper(text)):
        return None

    risks = _risk_types(text)
    sev = _severity(text, risks)

    # MVP: no real geocoding yet. Keep placeholder "Global" unless you add NER+geocode later.
    loc = location or {"name": "Global", "lat": 0.0, "lon": 0.0, "precision": "country"}

    why = _why_it_matters(risks)

    return {
        "id": _id(url + "|" + title),
        "title": (title or "")[:500],
        "summary": (summary or "")[:240],
        "whyItMatters": why,
        "sourceUrl": url,
        "sourceName": source_name,
        "sourceQuality": _quality_from_url(url),
        "publishedAt": published_at,
        "materials": ["Copper"],
        "riskType": risks,
        "severity": sev,
        "location": loc,
        "tags": sorted(list({*(r.upper() for r in risks), "COPPER"})),
    }


def _parse_dt_to_z(dt_str: str) -> str:
    if not dt_str:
        return _nowz()
    try:
        return dtparse.parse(dt_str).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return _nowz()


# ----------------------------
# Source ingestors
# ----------------------------
def ingest_mining_rss() -> List[Dict[str, Any]]:
    FEED_URL = "https://www.mining.com/feed/"

    r = requests.get(
        FEED_URL,
        timeout=(5, 20),
        headers={"User-Agent": "geo-monitor/1.0", "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"},
    )
    r.raise_for_status()

    # IMPORTANT: pass bytes, not decoded text
    feed = feedparser.parse(r.content)

    # feedparser doesn't throw; it sets bozo flag on parse issues
    if getattr(feed, "bozo", False):
        # Don’t kill ingestion—just skip Mining.com if it’s malformed today
        print(f"[Mining.com] RSS parse error (bozo): {getattr(feed, 'bozo_exception', None)}", flush=True)
        return []

    entries = getattr(feed, "entries", []) or []
    if not entries:
        print("[Mining.com] RSS returned 0 entries.", flush=True)
        return []

    out: List[Dict[str, Any]] = []
    for e in entries[:200]:
        title = getattr(e, "title", "") or ""
        url = getattr(e, "link", "") or ""
        summary = getattr(e, "summary", "") or ""
        published = getattr(e, "published", "") or ""

        published_at = _parse_dt_to_z(published)
        ev = _mk_event(title, summary, url, "Mining.com", published_at)
        if ev:
            out.append(ev)

    return out


def ingest_gdelt(days: int = 7) -> List[Dict[str, Any]]:
    # Make copper mandatory so results aren't diluted
    q = (
        "copper AND ("
        "mine OR mining OR smelter OR refinery OR concentrate OR cathode OR "
        "export OR import OR tariff OR quota OR strike OR protest OR "
        "sanction OR embargo OR port OR shipping OR canal OR blockade OR "
        "royalty OR regulation OR law"
        ")"
    )

    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": q,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": 100,
        "timespan": f"{days}d",
        "sort": "DateDesc",
    }

    r = requests.get(url, params=params, timeout=(5, 20), headers={"User-Agent": "geo-monitor/1.0"})
    r.raise_for_status()
    js = r.json()

    # GDELT can return "status": "error" with HTTP 200
    if js.get("status") == "error":
        raise RuntimeError(f"GDELT error: {js.get('message') or js}")

    articles = js.get("articles") or []
    if not articles:
        raise RuntimeError("GDELT returned 0 articles for the query/timespan.")

    out: List[Dict[str, Any]] = []
    for a in articles[:100]:
        title = a.get("title") or ""
        url0 = a.get("url") or ""
        summary = a.get("snippet") or ""
        seen = a.get("seendate") or _nowz()
        source = a.get("domain") or "GDELT"

        ev = _mk_event(title, summary, url0, source, seen)
        if ev:
            out.append(ev)

    return out


def ingest_gdelt_domain(days: int, domain: str, source_label: str) -> List[Dict[str, Any]]:
    q = (
        f"domain:{domain} AND copper AND ("
        "mine OR mining OR smelter OR refinery OR concentrate OR cathode OR "
        "export OR import OR tariff OR quota OR strike OR protest OR "
        "sanction OR embargo OR port OR shipping OR canal OR blockade OR "
        "royalty OR regulation OR law"
        ")"
    )

    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": q,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": 100,
        "timespan": f"{days}d",
        "sort": "DateDesc",
    }

    r = requests.get(url, params=params, timeout=(5, 20), headers={"User-Agent": "geo-monitor/1.0"})
    r.raise_for_status()
    js = r.json()

    if js.get("status") == "error":
        raise RuntimeError(f"GDELT error: {js.get('message') or js}")

    articles = js.get("articles") or []
    out: List[Dict[str, Any]] = []
    for a in articles[:100]:
        title = a.get("title") or ""
        url0 = a.get("url") or ""
        summary = a.get("snippet") or ""
        seen = a.get("seendate") or _nowz()

        ev = _mk_event(title, summary, url0, source_label, seen)
        if ev:
            out.append(ev)

    return out


def scrape_treasury_press_releases() -> List[Dict[str, Any]]:
    """
    Official Treasury press releases list page (HTML).
    Best-effort scrape: extract press release links and titles.
    """
    url = "https://home.treasury.gov/news/press-releases"
    r = requests.get(url, timeout=(5, 20), headers={"User-Agent": "geo-monitor/1.0"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    out: List[Dict[str, Any]] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)

        if not text:
            continue

        # Only keep actual press release detail pages
        if "/news/press-releases/" in href and len(text) > 15:
            full = href if href.startswith("http") else "https://home.treasury.gov" + href
            # Treasury press releases may matter even if "copper" isn't mentioned.
            # Keep only if it's geopolicy-relevant OR copper-relevant.
            risks = _risk_types(text)
            force = (risks != ["other"]) or _is_copper(text)
            ev = _mk_event(text, "", full, "U.S. Treasury", _nowz(), force_include=force)
            if ev:
                out.append(ev)

    return out[:60]


def scrape_ofac_recent_actions() -> List[Dict[str, Any]]:
    """
    OFAC RSS is retired; use the official Recent Actions page.
    This source is known to be slow sometimes, so it MUST be best-effort and never crash ingestion.
    """
    url = "https://ofac.treasury.gov/recent-actions"
    try:
        r = requests.get(url, timeout=(5, 15), headers={"User-Agent": "geo-monitor/1.0"})
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[OFAC] skipped due to request error: {e}", flush=True)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    out: List[Dict[str, Any]] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)

        if not text or len(text) <= 12:
            continue

        # Recent Actions detail pages
        if "/recent-actions/" in href:
            full = href if href.startswith("http") else "https://ofac.treasury.gov" + href
            # OFAC is inherently sanctions/policy-oriented; include even if "copper" isn't in the title.
            ev = _mk_event(text, "", full, "OFAC", _nowz(), force_include=True)
            if ev:
                out.append(ev)

    return out[:60]


def ingest_state_rss() -> List[Dict[str, Any]]:
    """
    State Dept provides RSS feeds, but you must choose actual feed URLs and put them in STATE_DEPT_FEEDS.
    If empty, return [] without error.
    """
    if not STATE_DEPT_FEEDS:
        print("[StateDept] No RSS feeds configured (STATE_DEPT_FEEDS empty). Skipping.", flush=True)
        return []

    out: List[Dict[str, Any]] = []
    for feed_url in STATE_DEPT_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for e in getattr(feed, "entries", [])[:80]:
                title = getattr(e, "title", "") or ""
                url = getattr(e, "link", "") or ""
                summary = getattr(e, "summary", "") or ""
                published = getattr(e, "published", "") or ""
                published_at = _parse_dt_to_z(published)

                ev = _mk_event(title, summary, url, "U.S. State Dept", published_at)
                if ev:
                    out.append(ev)
        except Exception as e:
            print(f"[StateDept] feed failed {feed_url}: {e}", flush=True)

    return out


# ----------------------------
# Orchestration (safe / fault tolerant)
# ----------------------------

def run_all(days: int = 7) -> List[Dict[str, Any]]:
    """
    Runs ingestion across all enabled sources in a fault-tolerant way.
    - Never raises on a single source failure.
    - Stores per-source status in run_all.last_report.
    """
    sources: List[Tuple[str, Callable[[], List[Dict[str, Any]]]]] = [
        ("GDELT", lambda: ingest_gdelt(days)),
        ("Mining.com RSS", ingest_mining_rss),
        # ("Mining.com via GDELT", lambda: ingest_gdelt_domain(days, "mining.com", "Mining.com")),
        ("Treasury PR", scrape_treasury_press_releases),
        ("OFAC Recent Actions", scrape_ofac_recent_actions),
        ("State Dept RSS", ingest_state_rss),
    ]

    events: List[Dict[str, Any]] = []
    report: List[Dict[str, Any]] = []

    for name, fn in sources:
        try:
            items = fn()
            report.append({"name": name, "events": len(items), "error": None})
            events.extend(items)
        except Exception as e:
            report.append({"name": name, "events": 0, "error": f"{type(e).__name__}: {e}"})
            print(f"[{name}] skipped due to error: {e}", flush=True)

    # de-dupe by id
    by_id: Dict[str, Dict[str, Any]] = {e["id"]: e for e in events}
    out = list(by_id.values())

    # attach report for API response
    run_all.last_report = report  # type: ignore[attr-defined]
    return out
