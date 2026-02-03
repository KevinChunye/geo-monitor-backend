# ingest.py
"""
Ingestion layer for the Geopolitical/Policy Monitor.

Design goals:
- NEVER crash the whole ingestion if one source fails (skip & continue).
- Best-effort HTTP with retries/backoff; treat blocks (403/451) as "blocked" and skip.
- Normalize events into a stable schema for the frontend.
- Lightweight country + lat/lon enrichment (regex hints -> ISO2 + centroid).

Current sources:
- GDELT 2.1 DOC API (discovery)
- Mining.com RSS (industry)
- U.S. Treasury press releases (HTML list scrape)
- OFAC recent actions (HTML list scrape; best-effort)
- EU Council press releases (RSS)
- UK OFSI blog (RSS)
- U.S. State Dept RSS (optional; configure STATE_DEPT_FEEDS)
"""

from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Tuple, Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparse


# ----------------------------
# Exceptions
# ----------------------------

class SourceBlocked(RuntimeError):
    """Raised when a source appears to block automated access (403/451)."""


# ----------------------------
# Configuration
# ----------------------------

COPPER_KW = [
    "copper", "codelco", "smelter", "smelting", "refining", "refinery",
    "concentrate", "cathode", "mine", "mining", "tcrc", " cu "
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
    "www.consilium.europa.eu": "OFFICIAL",
    "consilium.europa.eu": "OFFICIAL",
    "ofsi.blog.gov.uk": "OFFICIAL",
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

# NOTE: You MUST fill STATE_DEPT_FEEDS with actual State Dept RSS feed URLs.
STATE_DEPT_FEEDS: List[str] = []

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GeoMonitorBot/1.0; +https://critical-material-geotracking.onrender.com)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}



# ----------------------------
# Entity & region geo enrichment (higher precision than country keyword)
# ----------------------------
# Strategy (conservative):
# 1) ENTITY_GEO_HINTS (substring match on normalized text) -> precision="entity"
# 2) REGION_HINTS (regex match on provinces/mining regions) -> precision="region"
# 3) COUNTRY_HINTS (regex match) -> precision="country"
# 4) else -> Global fallback

# Format: (substring_keyword_lower, ISO2 list, lat, lon)
# NOTE: Keep keywords lowercase; matching happens on text.lower().
ENTITY_GEO_HINTS: List[Tuple[str, List[str], float, float]] = [
    # --- Africa / Copperbelt / DRC ---
    ("kamoa-kakula", ["CD"], -10.7390, 25.4580),
    ("kamoa", ["CD"], -10.7390, 25.4580),
    ("kakula", ["CD"], -10.7390, 25.4580),
    ("ivanhoe", ["CD"], -10.7390, 25.4580),
    ("tenke fungurume", ["CD"], -10.9850, 26.7420),
    ("tenke", ["CD"], -10.9850, 26.7420),
    ("fungurume", ["CD"], -10.9850, 26.7420),
    ("kolwezi", ["CD"], -10.7167, 25.4667),
    ("lubumbashi", ["CD"], -11.6609, 27.4794),
    ("kansanshi", ["ZM"], -12.0950, 26.4270),
    ("lumwana", ["ZM"], -12.2580, 25.7990),
    ("mopani", ["ZM"], -12.5490, 27.8500),
    ("konkola", ["ZM"], -12.3790, 27.8240),
    ("first quantum", ["ZM"], -12.0950, 26.4270),  # Kansanshi/Lumwana anchor
    ("cmoc", ["CD"], -10.9850, 26.7420),  # Tenke Fungurume (CMOC)
    ("china molybdenum", ["CD"], -10.9850, 26.7420),
    ("barrick", ["CD"], -10.9850, 26.7420),  # via JV footprint; conservative centroid
    ("anglo american", ["ZA"], -30.5595, 22.9375),  # corporate anchor when no asset hint
    ("kumba", ["ZA"], -30.5595, 22.9375),

    # --- Gems example (your case) ---
    ("gemfields", ["MZ"], -18.6657, 35.5296),  # Mozambique (Montepuez ruby)

    # --- Chile / Peru copper assets ---
    ("codelco", ["CL"], -35.6751, -71.5430),
    ("escondida", ["CL"], -24.2640, -69.0780),
    ("collahuasi", ["CL"], -20.9560, -68.6500),
    ("los pelambres", ["CL"], -31.8833, -70.6333),
    ("antofagasta minerals", ["CL"], -23.6500, -70.4000),
    ("sqm", ["CL"], -23.6500, -70.4000),
    ("quellaveco", ["PE"], -16.9920, -70.8550),
    ("las bambas", ["PE"], -14.7220, -71.3680),
    ("antamina", ["PE"], -9.7450, -77.1970),
    ("southern copper", ["PE"], -17.6390, -71.3370),
    ("freeport-mcmoran", ["US"], 39.8283, -98.5795),  # fallback corporate anchor
    ("freeport", ["US"], 39.8283, -98.5795),

    # --- North America smelting/refining anchors ---
    ("glencore horne", ["CA"], 48.2366, -79.0217),  # Rouyn-Noranda
    ("horne smelter", ["CA"], 48.2366, -79.0217),
    ("teck", ["CA"], 56.1304, -106.3468),
    ("hudbay", ["CA"], 56.1304, -106.3468),
    ("foran mining", ["CA"], 52.9399, -106.4509),
    ("first majestic", ["CA"], 56.1304, -106.3468),

    # --- Indonesia / Papua ---
    ("grasberg", ["ID"], -4.0530, 137.1160),
    ("pt freeport indonesia", ["ID"], -4.0530, 137.1160),

    # --- Australia ---
    ("bhp", ["AU"], -25.2744, 133.7751),
    ("rio tinto", ["AU"], -25.2744, 133.7751),
    ("newmont", ["US"], 39.8283, -98.5795),  # corporate anchor
]

# Format: (regex_pattern, ISO2, lat, lon)
REGION_HINTS: List[Tuple[str, str, float, float]] = [
    # Canada
    (r"\bbritish columbia\b|\bbc\b", "CA", 53.7267, -127.6476),
    (r"\bquebec\b", "CA", 52.9399, -73.5491),
    (r"\bontario\b", "CA", 51.2538, -85.3232),
    (r"\bsaskatchewan\b", "CA", 52.9399, -106.4509),

    # US
    (r"\barizona\b", "US", 34.0489, -111.0937),
    (r"\bnevada\b", "US", 38.8026, -116.4194),
    (r"\bnew mexico\b", "US", 34.5199, -105.8701),

    # Australia
    (r"\bwestern australia\b|\bwa\b", "AU", -27.6728, 121.6283),
    (r"\bqueensland\b", "AU", -20.9176, 142.7028),
    (r"\bnew south wales\b", "AU", -31.2532, 146.9211),

    # Africa mining regions
    (r"\bcopperbelt\b", "ZM", -12.0000, 27.0000),
    (r"\bkatanga\b|\bhaut-katanga\b", "CD", -11.6600, 27.4800),
]
# ----------------------------
# Lightweight geo enrichment
# ----------------------------
# Format: (regex_pattern, ISO2, lat, lon)
COUNTRY_HINTS: List[Tuple[str, str, float, float]] = [
    (r"\bunited states\b|\bu\.s\.\b|\busa\b|\bamerica\b", "US", 39.8283, -98.5795),
    (r"\bcanada\b", "CA", 56.1304, -106.3468),
    (r"\bmexico\b", "MX", 23.6345, -102.5528),
    (r"\bchile\b", "CL", -35.6751, -71.5430),
    (r"\bperu\b", "PE", -9.1900, -75.0152),
    (r"\bargentina\b", "AR", -38.4161, -63.6167),
    (r"\bbolivia\b", "BO", -16.2902, -63.5887),
    (r"\bbrazil\b", "BR", -14.2350, -51.9253),
    (r"\bcolombia\b", "CO", 4.5709, -74.2973),
    (r"\becuador\b", "EC", -1.8312, -78.1834),
    (r"\bpanama\b", "PA", 8.5380, -80.7821),
    (r"\bvenezuela\b", "VE", 6.4238, -66.5897),
    (r"\bguatemala\b", "GT", 15.7835, -90.2308),
    (r"\bhonduras\b", "HN", 15.2000, -86.2419),

    (r"\bunited kingdom\b|\buk\b|\bbritain\b|\bengland\b", "GB", 55.3781, -3.4360),
    (r"\beuropean union\b|\beu\b", "EU", 50.1109, 8.6821),  # placeholder (Frankfurt)
    (r"\bfrance\b", "FR", 46.2276, 2.2137),
    (r"\bgermany\b", "DE", 51.1657, 10.4515),
    (r"\bitaly\b", "IT", 41.8719, 12.5674),
    (r"\bspain\b", "ES", 40.4637, -3.7492),
    (r"\bportugal\b", "PT", 39.3999, -8.2245),
    (r"\bnetherlands\b|\bholland\b", "NL", 52.1326, 5.2913),
    (r"\bbelgium\b", "BE", 50.5039, 4.4699),
    (r"\bpoland\b", "PL", 51.9194, 19.1451),
    (r"\bsweden\b", "SE", 60.1282, 18.6435),
    (r"\bnorway\b", "NO", 60.4720, 8.4689),
    (r"\bfinland\b", "FI", 61.9241, 25.7482),
    (r"\bukraine\b", "UA", 48.3794, 31.1656),
    (r"\brussia\b|\brussian\b", "RU", 61.5240, 105.3188),
    (r"\bserbia\b", "RS", 44.0165, 21.0059),
    (r"\bmontenegro\b", "ME", 42.7087, 19.3744),

    (r"\bchina\b|\bprc\b", "CN", 35.8617, 104.1954),
    (r"\btaiwan\b|\btaipei\b", "TW", 23.6978, 120.9605),
    (r"\bjapan\b", "JP", 36.2048, 138.2529),
    (r"\bkorea\b|\bsouth korea\b", "KR", 35.9078, 127.7669),
    (r"\bindia\b", "IN", 20.5937, 78.9629),
    (r"\bvietnam\b", "VN", 14.0583, 108.2772),
    (r"\bindonesia\b", "ID", -0.7893, 113.9213),
    (r"\bphilippines\b", "PH", 12.8797, 121.7740),
    (r"\bmalaysia\b", "MY", 4.2105, 101.9758),
    (r"\bthailand\b", "TH", 15.8700, 100.9925),
    (r"\bsingapore\b", "SG", 1.3521, 103.8198),
    (r"\baustralia\b", "AU", -25.2744, 133.7751),
    (r"\bnew zealand\b", "NZ", -40.9006, 174.8860),

    (r"\bturkey\b|\bturkiye\b", "TR", 38.9637, 35.2433),
    (r"\biran\b", "IR", 32.4279, 53.6880),
    (r"\biraq\b", "IQ", 33.2232, 43.6793),
    (r"\bsaudi\b|\bsaudi arabia\b", "SA", 23.8859, 45.0792),
    (r"\buae\b|\bunited arab emirates\b", "AE", 23.4241, 53.8478),
    (r"\bqatar\b", "QA", 25.3548, 51.1839),
    (r"\bisrael\b", "IL", 31.0461, 34.8516),
    (r"\bgaza\b|\bwest bank\b|\bpalestin\w*\b", "PS", 31.9522, 35.2332),
    (r"\byemen\b", "YE", 15.5527, 48.5164),
    (r"\begypt\b", "EG", 26.8206, 30.8025),
    (r"\bsudan\b", "SD", 12.8628, 30.2176),
    (r"\bethiopia\b", "ET", 9.1450, 40.4897),
    (r"\bkenya\b", "KE", -0.0236, 37.9062),
    (r"\btanzania\b", "TZ", -6.3690, 34.8888),
    (r"\buganda\b", "UG", 1.3733, 32.2903),
    (r"\brwanda\b", "RW", -1.9403, 29.8739),
    (r"\bburundi\b", "BI", -3.3731, 29.9189),

    (r"\bdrc\b|\bdr congo\b|\bdemocratic republic of the congo\b|\bcongo-kinshasa\b", "CD", -4.0383, 21.7587),
    (r"\bcongo\b(?!-kinshasa)|\brepublic of the congo\b|\bcongo-brazzaville\b", "CG", -0.2280, 15.8277),
    (r"\bzambia\b", "ZM", -13.1339, 27.8493),
    (r"\bdr\.?c\.?\b", "CD", -4.0383, 21.7587),
    (r"\bbotswana\b", "BW", -22.3285, 24.6849),
    (r"\bghana\b", "GH", 7.9465, -1.0232),
    (r"\bnamibia\b", "NA", -22.9576, 18.4904),
    (r"\bmadagascar\b", "MG", -18.7669, 46.8691),
    (r"\bmorocco\b", "MA", 31.7917, -7.0926),
    (r"\balgeria\b", "DZ", 28.0339, 1.6596),
    (r"\btunisia\b", "TN", 33.8869, 9.5375),
    (r"\bnigeria\b", "NG", 9.0820, 8.6753),
    (r"\bsouth africa\b", "ZA", -30.5595, 22.9375),
    (r"\bmozambique\b", "MZ", -18.6657, 35.5296),
    (r"\bangola\b", "AO", -11.2027, 17.8739),
    (r"\bguinea\b", "GN", 9.9456, -9.6966),

    (r"\bchad\b", "TD", 15.4542, 18.7322),
    (r"\barmenia\b", "AM", 40.0691, 45.0382),
    (r"\baz\w*\b", "AZ", 40.1431, 47.5769),
]

_COUNTRY_HINTS_COMPILED: List[Tuple[re.Pattern, str, float, float]] = [
    (re.compile(pat, re.IGNORECASE), iso2, lat, lon) for pat, iso2, lat, lon in COUNTRY_HINTS
]
_REGION_HINTS_COMPILED: List[Tuple[re.Pattern, str, float, float]] = [
    (re.compile(pat, re.IGNORECASE), iso2, lat, lon) for pat, iso2, lat, lon in REGION_HINTS
]


def extract_geo(text: str) -> Tuple[List[str], Optional[Tuple[float, float]], str]:
    """Return (countries, centroid, precision).

    precision ∈ {"entity","region","country","global"}.

    Conservative policy:
    - If we can confidently infer from an entity/asset keyword -> entity centroid.
    - Else if a province/mining region matches -> region centroid.
    - Else fallback to country keyword centroid.
    - Else Global.
    """
    t = (text or "")
    tl = t.lower()

    # 1) Entity keywords (substring match)
    for key, iso2s, lat, lon in ENTITY_GEO_HINTS:
        if key and key in tl:
            return iso2s, (float(lat), float(lon)), "entity"

    # 2) Regions (regex)
    hits: List[str] = []
    centroid: Optional[Tuple[float, float]] = None
    for pat, iso2, lat, lon in _REGION_HINTS_COMPILED:
        if pat.search(t):
            if iso2 not in hits:
                hits.append(iso2)
            if centroid is None:
                centroid = (float(lat), float(lon))
    if hits or centroid:
        return hits, centroid, "region"

    # 3) Countries (regex)
    hits = []
    centroid = None
    for pat, iso2, lat, lon in _COUNTRY_HINTS_COMPILED:
        if pat.search(t):
            if iso2 not in hits:
                hits.append(iso2)
            if centroid is None:
                centroid = (float(lat), float(lon))
    if hits or centroid:
        return hits, centroid, "country"

    # 4) Global
    return [], None, "global"


# ----------------------------
# HTTP helper (robust)
# ----------------------------

_SESSION = requests.Session()

def _backoff(attempt: int) -> float:
    # 0.6, 1.2, 2.4, ... capped
    return min(6.0, 0.6 * (2 ** attempt))

def http_get(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Tuple[float, float] = (5, 20),
    max_retries: int = 3,
) -> Optional[requests.Response]:
    hdrs = dict(DEFAULT_HEADERS)
    if headers:
        hdrs.update(headers)

    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            r = _SESSION.get(url, params=params, headers=hdrs, timeout=timeout)
            if r.status_code in (403, 451):
                raise SourceBlocked(f"Blocked by {url} (HTTP {r.status_code})")
            if r.status_code == 429 or 500 <= r.status_code < 600:
                time.sleep(_backoff(attempt))
                continue
            r.raise_for_status()
            return r
        except SourceBlocked:
            # hard block -> skip source
            return None
        except requests.RequestException as e:
            last_exc = e
            time.sleep(_backoff(attempt))
            continue

    # Exhausted retries
    if last_exc:
        raise last_exc
    return None


# ----------------------------
# Generic helpers
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
    Simple heuristic severity score 0-100.
    """
    t = (text or "").lower()
    score = 25

    if any(x in t for x in ["shutdown", "collapse", "ban", "embargo"]):
        score += 40
    elif any(x in t for x in ["strike", "blockade", "attack", "sanction", "halt"]):
        score += 25
    elif any(x in t for x in ["delay", "protest", "tighten", "restriction"]):
        score += 10

    if "sanctions" in risks or "conflict" in risks:
        score += 10
    if "policy" in risks:
        score += 5
    if "logistics" in risks:
        score += 5

    return max(0, min(100, score))


def _parse_dt_to_z(dt_str: str) -> str:
    if not dt_str:
        return _nowz()
    try:
        return dtparse.parse(dt_str).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return _nowz()


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
        summary = " ".join(title.split()[:28])

    text = f"{title} {summary}"

    if (not force_include) and (not _is_copper(text)):
        return None

    risks = _risk_types(text)
    sev = _severity(text, risks)
    why = _why_it_matters(risks)

    countries, centroid, precision = extract_geo(text)

    # Location precedence:
    # 1) explicit location passed in from ingestor
    # 2) inferred country centroid
    # 3) Global fallback
    if location and isinstance(location, dict) and ("lat" in location and "lon" in location):
        loc = location
    elif centroid:
        lat, lon = centroid
        loc = {
            "name": (countries[0] if countries else "Global"),
            "lat": float(lat),
            "lon": float(lon),
            "precision": precision,
        }
    else:
        loc = {"name": "Global", "lat": 0.0, "lon": 0.0, "precision": "global"}

    return {
        "id": _id(url + "|" + title),
        "title": (title or "")[:500],
        "summary": (summary or "")[:240],
        "whyItMatters": why,
        "sourceUrl": url,
        "sourceName": source_name,
        "sourceQuality": _quality_from_url(url),
        "publishedAt": _parse_dt_to_z(published_at),
        "materials": ["Copper"],
        "riskType": risks,
        "severity": sev,
        "countries": countries or (["GLOBAL"] if loc["precision"] == "global" else []),
        "location": loc,
        "tags": sorted(list({*(r.upper() for r in risks), "COPPER"})),
    }


# ----------------------------
# Source ingestors
# ----------------------------

def ingest_mining_rss() -> List[Dict[str, Any]]:
    """
    Mining.com RSS.

    Mining.com frequently changes RSS endpoints, and some endpoints can return 403 to non-browser user agents.
    This ingestor:
    - Tries multiple candidate feed URLs
    - Uses a browser-like User-Agent
    - Never crashes ingestion if Mining.com blocks us
    """

    FEED_URLS = [
        "https://www.mining.com/feed/",
        "https://www.mining.com/?feed=rss2",
        "https://mining.com/feed/",
        "https://mining.com/?feed=rss2",
    ]

    headers = {
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    last_err: Exception | None = None

    for feed_url in FEED_URLS:
        try:
            r = http_get(feed_url, headers=headers, timeout=(5, 20))
            if r is None:
                # blocked
                return []
            feed = feedparser.parse(r.content)
            if getattr(feed, "bozo", False):
                continue
            entries = getattr(feed, "entries", []) or []
            if not entries:
                continue

            out: List[Dict[str, Any]] = []
            for e in entries[:200]:
                title = getattr(e, "title", "") or ""
                url = getattr(e, "link", "") or ""
                summary = getattr(e, "summary", "") or ""
                published = getattr(e, "published", "") or ""
                ev = _mk_event(title, summary, url, "Mining.com", published)
                if ev:
                    out.append(ev)
            return out
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise last_err
    return []


def ingest_gdelt(days: int = 7) -> List[Dict[str, Any]]:
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

    r = http_get(url, params=params, headers={"Accept": "application/json"})
    if r is None:
        return []
    js = r.json()
    if js.get("status") == "error":
        raise RuntimeError(f"GDELT error: {js.get('message') or js}")

    articles = js.get("articles") or []
    if not articles:
        return []

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

    r = http_get(url, params=params, headers={"Accept": "application/json"})
    if r is None:
        return []
    js = r.json()
    if js.get("status") == "error":
        raise RuntimeError(f"GDELT error: {js.get('message') or js}")

    articles = js.get("articles") or []
    if not articles:
        return []

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
    url = "https://home.treasury.gov/news/press-releases"
    r = http_get(url, timeout=(5, 20))
    if r is None:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    out: List[Dict[str, Any]] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if not text:
            continue
        if "/news/press-releases/" in href and len(text) > 15:
            full = href if href.startswith("http") else "https://home.treasury.gov" + href
            risks = _risk_types(text)
            force = (risks != ["other"]) or _is_copper(text)
            ev = _mk_event(text, "", full, "U.S. Treasury", _nowz(), force_include=force)
            if ev:
                out.append(ev)

    return out[:80]


def scrape_ofac_recent_actions() -> List[Dict[str, Any]]:
    url = "https://ofac.treasury.gov/recent-actions"
    r = http_get(url, timeout=(5, 15))
    if r is None:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    out: List[Dict[str, Any]] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if not text or len(text) <= 12:
            continue
        if "/recent-actions/" in href:
            full = href if href.startswith("http") else "https://ofac.treasury.gov" + href
            ev = _mk_event(text, "", full, "OFAC", _nowz(), force_include=True)
            if ev:
                out.append(ev)

    return out[:80]


def ingest_state_rss() -> List[Dict[str, Any]]:
    if not STATE_DEPT_FEEDS:
        return []

    out: List[Dict[str, Any]] = []
    for feed_url in STATE_DEPT_FEEDS:
        try:
            r = http_get(feed_url, headers={"Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"})
            if r is None:
                continue
            feed = feedparser.parse(r.content)
            for e in getattr(feed, "entries", [])[:200]:
                title = getattr(e, "title", "") or ""
                url = getattr(e, "link", "") or ""
                summary = getattr(e, "summary", "") or ""
                published = getattr(e, "published", "") or ""
                ev = _mk_event(title, summary, url, "U.S. State Dept", published)
                if ev:
                    out.append(ev)
        except Exception:
            continue
    return out


def ingest_eu_consilium_press_releases_rss() -> List[Dict[str, Any]]:
    feed_url = "https://www.consilium.europa.eu/en/rss/pressreleases.ashx"
    r = http_get(feed_url, headers={"Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"})
    if r is None:
        return []

    feed = feedparser.parse(r.content)
    if getattr(feed, "bozo", False):
        return []

    out: List[Dict[str, Any]] = []
    for e in getattr(feed, "entries", [])[:200]:
        title = getattr(e, "title", "") or ""
        url = getattr(e, "link", "") or ""
        summary = getattr(e, "summary", "") or ""
        published = getattr(e, "published", "") or ""
        ev = _mk_event(title, summary, url, "EU Council", published, force_include=True)
        if ev:
            out.append(ev)
    return out


def ingest_uk_ofsi_blog_feed() -> List[Dict[str, Any]]:
    feed_url = "https://ofsi.blog.gov.uk/feed/"
    r = http_get(feed_url, headers={"Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"})
    if r is None:
        return []

    feed = feedparser.parse(r.content)
    if getattr(feed, "bozo", False):
        return []

    out: List[Dict[str, Any]] = []
    for e in getattr(feed, "entries", [])[:200]:
        title = getattr(e, "title", "") or ""
        url = getattr(e, "link", "") or ""
        summary = getattr(e, "summary", "") or ""
        published = getattr(e, "published", "") or ""
        ev = _mk_event(title, summary, url, "UK OFSI", published, force_include=True)
        if ev:
            out.append(ev)
    return out


# ----------------------------
# Master run
# ----------------------------

def run_all(days: int = 7) -> List[Dict[str, Any]]:
    """
    Run all enabled sources and return merged events.
    Attaches run_all.last_report for debugging.
    """
    sources: List[Tuple[str, Callable[[], List[Dict[str, Any]]]]] = [
        ("GDELT", lambda: ingest_gdelt(days=days)),
        ("Mining.com RSS", ingest_mining_rss),
        ("U.S. Treasury", scrape_treasury_press_releases),
        ("OFAC", scrape_ofac_recent_actions),
        ("EU Council", ingest_eu_consilium_press_releases_rss),
        ("UK OFSI", ingest_uk_ofsi_blog_feed),
        ("U.S. State Dept", ingest_state_rss),
        ("Reuters via GDELT", lambda: ingest_gdelt_domain(days=days, domain="reuters.com", source_label="Reuters")),
    ]

    merged: Dict[str, Dict[str, Any]] = {}
    report: List[Dict[str, Any]] = []

    for name, fn in sources:
        t0 = time.time()
        try:
            events = fn() or []
            for e in events:
                merged[e["id"]] = e

            status = "ok" if events else "empty"
            report.append({
                "source": name,
                "status": status,
                "events": len(events),
                "error": None,
                "duration_ms": int((time.time() - t0) * 1000),
            })
        except SourceBlocked as e:
            report.append({
                "source": name,
                "status": "blocked",
                "events": 0,
                "error": str(e),
                "duration_ms": int((time.time() - t0) * 1000),
            })
        except Exception as e:
            report.append({
                "source": name,
                "status": "error",
                "events": 0,
                "error": str(e),
                "duration_ms": int((time.time() - t0) * 1000),
            })

    run_all.last_report = report  # type: ignore[attr-defined]
    return sorted(list(merged.values()), key=lambda x: x.get("publishedAt", ""), reverse=True)
