import sqlite3, json
from typing import List, Dict, Any

DB = "events.db"

def init_db():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      payload TEXT NOT NULL,
      published_at TEXT NOT NULL,
      source_quality TEXT NOT NULL
    )
    """)
    conn.commit()
    conn.close()

def upsert_events(events: List[Dict[str, Any]]):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    for e in events:
        cur.execute(
            "INSERT OR REPLACE INTO events (id, payload, published_at, source_quality) VALUES (?, ?, ?, ?)",
            (e["id"], json.dumps(e, ensure_ascii=False), e["publishedAt"], e.get("sourceQuality","OTHER"))
        )
    conn.commit()
    conn.close()

def query_events(material: str, since_iso: str, risk: str|None, quality: str|None, limit: int = 200):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT payload FROM events WHERE published_at >= ? ORDER BY published_at DESC LIMIT ?", (since_iso, limit))
    rows = [json.loads(r[0]) for r in cur.fetchall()]
    conn.close()

    out = []
    for e in rows:
        if material.lower() not in [m.lower() for m in e.get("materials", [])]:
            continue
        if risk and risk.lower() not in [r.lower() for r in e.get("riskType", [])]:
            continue
        if quality and quality.upper() != e.get("sourceQuality","OTHER"):
            continue
        out.append(e)
    return out
