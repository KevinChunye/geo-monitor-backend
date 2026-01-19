import json
import sqlite3
from typing import Any, Dict, List, Optional, Set

DB = "events.db"


def init_db() -> None:
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
          id TEXT PRIMARY KEY,
          payload TEXT NOT NULL,
          published_at TEXT NOT NULL,
          source_quality TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def upsert_events(events: List[Dict[str, Any]]) -> None:
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    for e in events:
        cur.execute(
            "INSERT OR REPLACE INTO events (id, payload, published_at, source_quality) VALUES (?, ?, ?, ?)",
            (
                e["id"],
                json.dumps(e, ensure_ascii=False),
                e["publishedAt"],
                e.get("sourceQuality", "OTHER"),
            ),
        )
    conn.commit()
    conn.close()


def query_events(
    material: str,
    since_iso: str,
    risk: Optional[str],
    qualities: Optional[Set[str]],
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """Query events.

    Notes:
    - We keep the SQL query simple (date-order + limit) and filter in Python.
    - `qualities` can be None to include all; otherwise it's a set of allowed quality labels.
    """

    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute(
        "SELECT payload FROM events WHERE published_at >= ? ORDER BY published_at DESC LIMIT ?",
        (since_iso, limit),
    )
    rows = [json.loads(r[0]) for r in cur.fetchall()]
    conn.close()

    mat = (material or "").lower()
    risk_l = (risk or "").lower() if risk else None
    qset = {q.upper() for q in qualities} if qualities else None

    out: List[Dict[str, Any]] = []
    for e in rows:
        if mat and mat not in [m.lower() for m in e.get("materials", [])]:
            continue
        if risk_l and risk_l not in [r.lower() for r in e.get("riskType", [])]:
            continue
        if qset and e.get("sourceQuality", "OTHER").upper() not in qset:
            continue
        out.append(e)

    return out
