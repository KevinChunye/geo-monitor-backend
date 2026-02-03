import json
import sqlite3
from typing import Any, Dict, List, Optional, Set, Tuple

DB = "events.db"


def _table_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def init_db() -> None:
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Base table (compatible with old deployments)
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

    cols = _table_columns(conn, "events")

    # Add frontend-friendly filter columns if missing (SQLite supports ADD COLUMN)
    def add_col(name: str, ddl: str) -> None:
        nonlocal cols
        if name not in cols:
            cur.execute(f"ALTER TABLE events ADD COLUMN {ddl}")
            conn.commit()
            cols = _table_columns(conn, "events")

    add_col("material", "material TEXT")
    add_col("risk_types", "risk_types TEXT")
    add_col("country_codes", "country_codes TEXT")
    add_col("lat", "lat REAL")
    add_col("lon", "lon REAL")

    # Indexes for fast filtering
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_published_at ON events(published_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_material ON events(material)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_source_quality ON events(source_quality)")
    conn.commit()
    conn.close()


def upsert_events(events: List[Dict[str, Any]]) -> None:
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    for e in events:
        material = (e.get("materials") or [""])[0]
        risks = ",".join(e.get("riskType") or [])
        countries = ",".join(e.get("countries") or [])
        loc = e.get("location") or {}
        lat = float(loc.get("lat") or 0.0)
        lon = float(loc.get("lon") or 0.0)

        cur.execute(
            """
            INSERT OR REPLACE INTO events
              (id, payload, published_at, source_quality, material, risk_types, country_codes, lat, lon)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                e["id"],
                json.dumps(e, ensure_ascii=False),
                e.get("publishedAt") or "",
                e.get("sourceQuality", "OTHER"),
                material,
                risks,
                countries,
                lat,
                lon,
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
    cursor: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Query events with lightweight SQL filtering + cursor pagination.

    cursor:
      - an ISO timestamp; returns events strictly older than cursor.

    Returns:
      (events, next_cursor)
    """
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    where = ["published_at >= ?"]
    args: List[Any] = [since_iso]

    if material:
        where.append("LOWER(material) = LOWER(?)")
        args.append(material)

    if risk:
        where.append("LOWER(risk_types) LIKE ?")
        args.append(f"%{risk.lower()}%")

    if qualities:
        qmarks = ",".join(["?"] * len(qualities))
        where.append(f"UPPER(source_quality) IN ({qmarks})")
        args.extend([q.upper() for q in qualities])

    if cursor:
        where.append("published_at < ?")
        args.append(cursor)

    sql = f"""
      SELECT payload, published_at
      FROM events
      WHERE {" AND ".join(where)}
      ORDER BY published_at DESC
      LIMIT ?
    """
    args.append(limit)

    cur.execute(sql, args)
    rows = cur.fetchall()
    conn.close()

    events: List[Dict[str, Any]] = []
    next_cursor = None
    for payload, published_at in rows:
        events.append(json.loads(payload))
        next_cursor = published_at  # last row's timestamp

    return events, next_cursor
