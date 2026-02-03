import traceback
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from store import init_db, upsert_events, query_events
from ingest import run_all

app = FastAPI(title="Geo Monitor API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"service": "Geo Monitor API", "docs": "/docs", "health": "/health"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.on_event("startup")
def _startup():
    init_db()


@app.post("/ingest/run")
def ingest_run(days: int = Query(7, ge=1, le=90)):
    try:
        events = run_all(days=days)
        upsert_events(events)
        report = getattr(run_all, "last_report", None)
        return {"status": "success", "events_ingested": len(events), "sources": report}
    except Exception as e:
        tb = traceback.format_exc()
        raise HTTPException(status_code=500, detail={"error": str(e), "traceback": tb})


@app.get("/events")
def get_events(
    material: str = Query("copper"),
    days: int = Query(30, ge=1, le=90),
    risk: str | None = Query(None),
    quality: str | None = Query("OFFICIAL,MAJOR_MEDIA,INDUSTRY"),
    limit: int = Query(200, ge=1, le=500),
    cursor: str | None = Query(None, description="ISO timestamp cursor for pagination; returns events older than cursor"),
):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat().replace("+00:00", "Z")

    qualities = None
    if quality and quality.strip().upper() != "ALL":
        qualities = {q.strip().upper() for q in quality.split(",") if q.strip()}

    evs, next_cursor = query_events(material, cutoff, risk, qualities, limit=limit, cursor=cursor)

    markers = 0
    for e in evs:
        loc = e.get("location") or {}
        lat = float(loc.get("lat") or 0.0)
        lon = float(loc.get("lon") or 0.0)
        if not (lat == 0.0 and lon == 0.0):
            markers += 1

    return {
        "events": evs,
        "count": len(evs),
        "markersCount": markers,
        "lastUpdated": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "nextCursor": next_cursor,
        "filters_applied": {
            "material": material,
            "days": days,
            "risk": risk,
            "quality": quality,
            "limit": limit,
            "cursor": cursor,
        },
    }
