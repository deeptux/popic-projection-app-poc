# Deploying to Vercel

This project has a **frontend** (Angular) and a **backend** (FastAPI). Vercel is used to deploy the **frontend** only. The backend must be hosted elsewhere (e.g. Railway, Render, Fly.io) and the frontend configured to call it.

## 1. Deploy the frontend to Vercel

### Option A: Deploy with Vercel Dashboard (recommended)

1. **Push your code** to GitHub / GitLab / Bitbucket (if not already).

2. Go to [vercel.com](https://vercel.com) and sign in. Click **Add New** → **Project**.

3. **Import** your repository `popic-projection-app-poc`.

4. **Configure the project:**
   - **Root Directory:** Click **Edit** and set to `frontend`.
   - **Framework Preset:** Other (or leave as detected).
   - **Build Command:** `npm run build` (default).
   - **Output Directory:** `dist/frontend/browser` (required for Angular).
   - **Install Command:** `npm install` (default).

5. Click **Deploy**. Vercel will install deps, run `npm run build`, and serve the app from `dist/frontend/browser`.

### Option B: Deploy with Vercel CLI

```bash
# Install Vercel CLI
npm i -g vercel

# From repo root, deploy the frontend
cd frontend
vercel

# Follow prompts: link to existing project or create new one.
# Root is the current directory (frontend); vercel.json already sets build and output.
```

The `frontend/vercel.json` in this repo already sets:

- `buildCommand`: `npm run build`
- `outputDirectory`: `dist/frontend/browser`
- `rewrites`: all routes → `/index.html` (for Angular client-side routing)

So with **Root Directory** = `frontend` in the Vercel project, you don’t need to set build/output in the UI.

---

## 2. If the build fails on Vercel (budgets)

The Angular app has strict bundle budgets. If the build fails with errors like “maximum budget exceeded”:

- In `frontend/angular.json`, under `projects.frontend.architect.build.configurations.production.budgets`, increase:
  - `maximumError` for `type: "initial"` (e.g. to `1.5MB`),
  - `maximumError` for `type: "anyComponentStyle"` (e.g. to `12kB`),
- Commit and push; Vercel will rebuild.

---

## 3. Backend and API URL

The frontend currently calls the API at `http://127.0.0.1:8000` (see `spreadsheets-page.ts` and `spreadsheets.effects.ts`).

To use the app on Vercel with a real backend:

1. **Host the backend** somewhere (e.g. Railway, Render) and get its URL (e.g. `https://your-api.railway.app`).
2. **Configure the frontend to use that URL:**
   - Either introduce an environment variable (e.g. `API_BASE_URL`) and use it in the frontend, building with that env set in Vercel’s **Environment Variables**, or
   - Replace the base URL in code with your production API URL (not ideal for multiple environments).
3. **CORS:** On the backend, allow your Vercel frontend origin (e.g. `https://your-app.vercel.app`) in `allow_origins` in `backend/main.py`.

After that, the deployed frontend will talk to your deployed backend.

---

## 4. Quick checklist

- [ ] Repo connected to Vercel, **Root Directory** = `frontend`
- [ ] Build command: `npm run build`, output: `dist/frontend/browser`
- [ ] If needed, relax Angular budgets in `angular.json`
- [ ] Backend deployed elsewhere; frontend uses that API URL (env or config)
- [ ] Backend CORS includes your Vercel frontend URL
