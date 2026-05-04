# POPIC Projection PoC — agent progress ledger

**Purpose:** Single living document for **General Inquiry**, **Code Planning**, and **Code Implementation** agents. Read this at the start of every session; update it when work materially changes the product (especially after implementation).

**Repo path:** `popic-projection-app-poc`  
**Last updated:** 2026-05-04 (Code Implementation — Vercel + Cloud Run deployment wiring)

---

## How each agent uses this file

| Agent | Read | Write |
| ----- | ---- | ----- |
| **General Inquiry** | Always, for factual project state | Optional: add “Inquiry / decisions” notes or correct inaccuracies |
| **Code Planning** | Always | Add/update **Planned work**, **Risks**, **Open questions**; adjust **Definition of done** when plans change |
| **Code Implementation** | Always before coding; again before ending session | **Required after each implementation session:** append a row to **Implementation log** and refresh **Current product snapshot** (and any section the change affects) |

**Implementation agent rule:** If you changed behavior, APIs, env vars, or run instructions, update this file in the **same commit / same PR** when possible.

---

## Deployment parameters (fill before first GCP / Vercel wire-up)

These are **not** stored in git; set in GitHub Actions **Variables**, Vercel **Environment Variables**, and GCP / WIF consoles.

| Parameter | Where to set | Example / notes |
| --------- | ------------- | ----------------- |
| GitHub `org/repo` | WIF attribute condition | Restrict OIDC to this repository only. |
| Deployment branch | [`.github/workflows/deploy-cloud-run.yml`](.github/workflows/deploy-cloud-run.yml) `on.push.branches` | Default in repo: `main` (edit if you use another branch). |
| GCP project ID | GitHub Variable `GCP_PROJECT_ID` | Required for `gcloud` and image path. |
| GCP region | GitHub Variable `GCP_REGION` | e.g. `us-central1` |
| Artifact Registry repo id | GitHub Variable `GCP_ARTIFACT_REGISTRY_REPO` | Docker repository name (not full URL). |
| Cloud Run service name | GitHub Variable `CLOUD_RUN_SERVICE` | e.g. `popic-projection-api` |
| WIF provider resource | GitHub Variable `GCP_WORKLOAD_IDENTITY_PROVIDER` | Full resource name from GCP IAM. |
| Deployer SA email | GitHub Variable `GCP_SERVICE_ACCOUNT` | SA used by `google-github-actions/auth`. |
| Vercel production hostname(s) | Cloud Run `CORS_ALLOWED_ORIGINS` | Comma-separated `https://…` origins (no trailing slash). Include custom domain if used. |
| Preview API strategy | Vercel **Preview** `POPIC_API_BASE_URL` + optional `CORS_ALLOW_ORIGIN_REGEX` on Cloud Run | Choose staging Cloud Run URL, production API URL, or omit preview API; if using many `*.vercel.app` previews, either list stable preview URL(s) in CORS or enable optional regex (broader exposure). |

---

## Current product snapshot (verified from repo)

### What this PoC is

Internal **POPIC LLC** proof-of-concept: ingest Excel reports, show tabular data, and drive **analytics charts** from uploaded/cleaned datasets. **Forecasting / NLP-driven categorization** are described in roadmap-style README text but are **not** evidenced as primary features in the current Angular pages surveyed.

### Tech stack (as implemented)

| Layer | Stack |
| ----- | ----- |
| **Frontend** | Angular **21** (standalone components), **Angular Material**, **Tailwind CSS v4**, **NgRx** (store + effects), **RxJS**, **Chart.js** via **ng2-charts** |
| **Backend** | **Python 3.12**, **FastAPI**, **Uvicorn**, **Polars** (+ fastexcel / xlsx stack), **spaCy** + `en_core_web_sm` in `requirements.txt` |
| **Tests** | Backend: `pytest` under `backend/tests/`; Frontend: Vitest via `@angular/build:unit-test` (browser runner packages may be required for `ng test`) |

### Local run

1. **Backend:** from `backend/`, venv + `pip install -r requirements.txt`, then `python main.py` (binds `127.0.0.1:8000`) or `uvicorn main:app --reload --host 127.0.0.1 --port 8000`.
2. **Frontend:** from `frontend/`, `npm ci`, `ng serve` → `http://localhost:4200/`. Uses [`environment.ts`](frontend/src/environments/environment.ts) (`apiBase`: `http://127.0.0.1:8000`).
3. **CORS:** Origins default to `http://localhost:4200` and `http://127.0.0.1:4200`, **plus** any origins listed in env `CORS_ALLOWED_ORIGINS` (comma-separated). Optional `CORS_ALLOW_ORIGIN_REGEX` for Starlette/FastAPI (e.g. Vercel preview pattern) — use only if stakeholders accept the risk.

### Cloud Run (container)

- **Image:** [`backend/Dockerfile`](backend/Dockerfile) — `python:3.12-slim-bookworm`, `pip install -r requirements.txt`, spaCy model from wheel in requirements (no runtime `spacy download`).
- **Listen:** `uvicorn main:app --host 0.0.0.0 --port ${PORT}` (`PORT` from Cloud Run, default `8080` in image).
- **Health:** `GET /health` → `{"status":"ok"}`.
- **Ingress (PoC):** GitHub workflow deploys with `--allow-unauthenticated` (public API). Tighten before production data.

### CI/CD

| Path | Role |
| ---- | ---- |
| [`.github/workflows/deploy-cloud-run.yml`](.github/workflows/deploy-cloud-run.yml) | On push to `main` (paths: `backend/**`, workflow file): WIF auth → Docker build in `backend/` → push to `REGION-docker.pkg.dev/PROJECT/REPO/SERVICE:SHA` → `gcloud run deploy`. **No** long-lived JSON keys. |
| **Vercel** | Connect repo; **Root Directory** `frontend/`; **Install** `npm ci`; **Build** `npm run build`; **Output Directory** `dist/frontend/browser`. Set `POPIC_API_BASE_URL` per environment (HTTPS, no trailing slash). [`frontend/vercel.json`](frontend/vercel.json) rewrites SPA routes to `index.html`. |

### Frontend structure (high level)

- **Routes** (`frontend/src/app/app.routes.ts`): shell `MainLayoutComponent` → `dashboard` (default redirect from `''`), `spreadsheets`.
- **State:** NgRx `spreadsheets` feature under `frontend/src/app/store/spreadsheets/`.
- **API base:** [`environment.ts`](frontend/src/environments/environment.ts) / [`environment.production.ts`](frontend/src/environments/environment.production.ts) (`apiBase`). Production file is overwritten at build time by [`frontend/scripts/set-api-base.mjs`](frontend/scripts/set-api-base.mjs) when env `POPIC_API_BASE_URL` is set (e.g. on Vercel). [`spreadsheets.effects.ts`](frontend/src/app/store/spreadsheets/spreadsheets.effects.ts) and [`spreadsheets-page.ts`](frontend/src/app/spreadsheets-page/spreadsheets-page.ts) import `environment`.

### Backend API (high level)

- **Root:** `GET /` — hello string. **`GET /health`** — JSON `{"status":"ok"}`.
- **Uploads / analytics:** unchanged surface; see [`backend/main.py`](backend/main.py).

### Environment variable matrix

| Variable | Where | Purpose |
| -------- | ----- | ------- |
| `PORT` | Cloud Run | Listen port (container). |
| `CORS_ALLOWED_ORIGINS` | Cloud Run | Comma-separated extra origins (merged with localhost defaults in app). |
| `CORS_ALLOW_ORIGIN_REGEX` | Cloud Run | Optional regex for additional allowed origins (e.g. Vercel previews). |
| `POPIC_API_BASE_URL` | Vercel (build), optional local | HTTPS API origin for production bundle; no trailing slash. `prebuild` writes `environment.production.ts`. |
| `GCP_*`, `CLOUD_RUN_SERVICE` | GitHub Actions Variables | See workflow file header comments. |

### Documentation vs repo (known drift)

- Root **`README.md`** may still describe Electron / different layout — treat as aspirational unless updated.
- **`backend/README.md`** may reference paths not in this tree.

---

## Uncommitted / in-flight

_Reconcile after your next commit (list was from older snapshots; current dirty set is whatever `git status` shows)._

---

## Planned work (Code Planning Agent — edit here)

| ID | Item | Priority | Notes |
| -- | ---- | -------- | ----- |
| P1 | Align root README with actual repo layout and run commands | Medium | Reduces onboarding confusion |
| P2 | API auth / lock down Cloud Run if PoC goes beyond internal | High | When leaving `--allow-unauthenticated` |

---

## Open questions / risks

- **Pytest:** `backend/tests/test_engine.py` had 4 failing tests in one local run (Salesforce ingest / merge expectations vs current engine). **Unrelated to deployment diff** — fix under a separate change if still failing on main.
- **`ng test`:** May require adding `@vitest/browser-*` per Angular 21 / Vitest integration message until browser packages are installed.
- **CORS:** Production Cloud Run must list every browser origin (Vercel prod + custom domain) or approved regex for previews.

---

## Inquiry / decisions (General Inquiry Agent — optional)

- 2026-05-04: Established this ledger and three-agent workflow; Code Planning prompt to be maintained from chat or linked instructions.

---

## Implementation log (append-only; Code Implementation Agent)

| Date (UTC) | Author / agent | Summary | Files / areas touched |
| ---------- | ---------------- | ------- | ---------------------- |
| 2026-05-04 | General Inquiry | Created `AGENT_PROJECT_PROGRESS.md`; documented stack, routes, API summary, README drift, dirty paths | `AGENT_PROJECT_PROGRESS.md` |
| 2026-05-04 | Code Implementation | Vercel + Cloud Run wiring: env CORS + `/health`, Dockerfile/.dockerignore, GHA WIF deploy workflow, Angular `POPIC_API_BASE_URL` prebuild + environments, `vercel.json`, removed debug `127.0.0.1:7300` fetch, CSS budget tweak for prod build | `backend/main.py`, `backend/Dockerfile`, `backend/.dockerignore`, `.github/workflows/deploy-cloud-run.yml`, `frontend/angular.json`, `frontend/package.json`, `frontend/vercel.json`, `frontend/scripts/set-api-base.mjs`, `frontend/src/environments/*`, `frontend/src/app/store/spreadsheets/spreadsheets.effects.ts`, `frontend/src/app/spreadsheets-page/spreadsheets-page.ts`, `frontend/src/app/shared/spreadsheet-data-table/spreadsheet-data-table.component.ts`, `AGENT_PROJECT_PROGRESS.md` |

---

## Definition of done (planning — edit as needed)

- [ ] User-facing behavior described matches code or is explicitly flagged “not built”.
- [ ] Run instructions work on a clean clone (Windows path in README still placeholder — fix or document).
- [ ] This ledger updated after implementation sessions.

---

## Smoke checklist (post-deploy)

1. `curl https://<cloud-run-url>/health` and `GET /`.
2. Open Vercel site `/` and `/spreadsheets`; hard refresh on `/spreadsheets`.
3. Upload + analytics flow in browser; DevTools: no CORS errors, API host is Cloud Run HTTPS.

**Rollback:** Cloud Run revision; Vercel → prior deployment.
