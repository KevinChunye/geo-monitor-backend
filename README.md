# Critical Materials Dashboard — Backend

Backend services powering the **Critical Materials** interactive dashboard (frontend hosted on Figma Site):
- Live site: https://miter-hungry-74922103.figma.site/

This repo provides the API + data pipelines that:
- ingest and normalize critical-materials signals (news/policy/market/industry sources),
- store structured entities (materials, countries, companies, events),
- expose clean endpoints consumed by the dashboard (and any other clients).

---

## What’s in here

### Core capabilities
- **API layer** (FastAPI): serves normalized data to the dashboard
- **Ingestion layer**: pulls from configured feeds/sources → parses → dedupes → normalizes
- **Processing layer**: enriches records (tagging, entity linking, scoring, categorization)
- **Storage**: persists raw + cleaned objects for reproducibility and auditing

### Typical data flow
`source → ingest → normalize → enrich/score → store → serve via API`

---

## Tech stack (edit to match your repo)
- **Python** (>= 3.10 recommended)
- **FastAPI** + Uvicorn
- **Database**: Postgres (recommended) or SQLite for local dev
- **Jobs**: cron / APScheduler / Celery (depending on your implementation)
- **Deployment**: Docker + (Render/Fly/EC2/etc.)

> If your repo differs (Node/Express, Django, etc.), replace this section with the real stack.

---
