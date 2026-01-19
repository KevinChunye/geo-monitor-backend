# main.py
import traceback
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta, timezone

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

@app.on_event("startup")
def _startup():
    init_db()

@app.post("/ingest/run")
def ingest_run():
    try:
        print("=== INGEST RUN START ===", flush=True)
        events = run_all()
        print(f"Ingested {len(events)} events", flush=True)

        upsert_events(events)

        report = getattr(run_all, "last_report", None)
        print("SOURCE REPORT:", report, flush=True)

        print("=== INGEST RUN END ===", flush=True)
        return {
            "status": "success",
            "events_ingested": len(events),
            "sources": report,
        }

    except Exception as e:
        tb = traceback.format_exc()
        print("=== INGEST RUN FAILED ===", flush=True)
        print(tb, flush=True)

        # Return traceback in response so Swagger shows it
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "traceback": tb
            }
        )

@app.get("/events")
def get_events(
    material: str = Query("copper"),
    days: int = Query(7, ge=1, le=30),
    risk: str | None = Query(None),
    # Default hides low-quality SEO/finance-blog spam. Set quality=ALL to include everything.
    quality: str | None = Query("OFFICIAL,MAJOR_MEDIA,INDUSTRY"),
    limit: int = Query(200, ge=1, le=500),
):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat().replace("+00:00","Z")

    qualities = None
    if quality:
        if quality.strip().upper() != "ALL":
            qualities = {q.strip().upper() for q in quality.split(",") if q.strip()}

    evs = query_events(material, cutoff, risk, qualities, limit=limit)
    return {
        "events": evs,
        "count": len(evs),
        "filters_applied": {
            "material": material,
            "days": days,
            "risk": risk,
            "quality": quality
        }
    }
