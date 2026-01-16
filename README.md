# POPIC LLC: Revenue Forecasting PoC

A high-performance desktop forecasting suite designed for **POPIC LLC**. This application integrates enterprise-grade UI components with a high-speed Rust-backed data engine to process complex Excel financial data and project future revenue streams.

## ⚡ Tech Stack & Environment

### Frontend (Desktop Client)

- **Framework:** Angular (Latest v21+)
- **UI Components:** **Angular Material** (Material 3 Design)
- **Styling:** **Tailwind CSS v4** (High-performance, CSS-first engine)
- **Runtime:** **ElectronJS** (Node.js **v24.12.x** environment)

### Backend (Analytical Engine)

- **Language:** **Python 3.12.x** (Optimized for performance and type-safety)
- **Framework:** **FastAPI** (Asynchronous REST API)
- **Processing:** **Polars** (High-performance, multi-threaded DataFrame library)
- **NLP:** **spaCy** (For intelligent categorization of spreadsheet line-items)
- **Distribution:** **PyInstaller** (Standalone backend binary)

---

## 🏗️ Architecture: The "Safe-Start" Lifecycle

To provide a seamless user experience, the app implements a strict **synchronous startup sequence** within a monorepo structure:

1.  **Process Spawn:** On launch, the Electron main process (Node 24.12) identifies the environment (dev vs. prod) and triggers the FastAPI backend (Python 3.12 / PyInstaller Binary).
2.  **Heartbeat Polling:** The Angular frontend is held in a "Loading" state while Electron performs asynchronous health checks against the backend’s `GET /health` endpoint.
3.  **UI Hydration:** Only once the backend responds with a `200 OK` (signaling Polars and spaCy models are loaded) does the Electron window render the Angular application.
4.  **Automatic Cleanup:** Closing the Electron window sends a termination signal to the Python subprocess to prevent "zombie" background processes.

---

## 📂 Project Structure

```text
/popic-forecast-suite
├── /apps
│   ├── /frontend        # Angular + Angular Material + Tailwind v4
│   └── /desktop         # Electron main process & startup logic
├── /backend             # Python 3.12 FastAPI service
│   ├── /api             # REST Endpoints
│   ├── /engine          # Polars data processing & spaCy NLP logic
│   └── main.py          # Entry point for PyInstaller bundling
├── /dist                # Compiled binaries and production builds
└── package.json         # Monorepo / NPM Workspaces management

```

## 🛠️ Development & Build

### Prerequisites

- **Node.js:** `v24.12.0`
- **Python:** `3.12.0`
- **C++ Build Tools:** (Required for Polars/spaCy compilation)

### Setup Instructions

1. **Clone & Install Node Modules**

   ```bash
   git clone https://github.com/popic-llc/popic-projection-app-poc.git
   cd popic-projection-app-poc
   npm install
   ```

2. **Setup Python Environment**

   ```bash
   # Navigate to backend directory
   cd backend

   # Create virtual environment
   "C:\**CHANGE_THIS[Users\Path]**\Programs\Python\Python312\python.exe" -m venv pyenv12

   # Activate virtual environment
   # On macOS/Linux:
   source pyenv12/bin/activate
   # On Windows:
   .\pyenv12\Scripts\activate

   # Install dependencies
   pip install -r requirements.txt

   # Download spaCy NLP model
   python -m spacy download en_core_web_sm
   ```

3. **Running the Application**

   #### Development Mode

   To run with hot-reloading (requires two terminals):

   **Terminal 1 (Backend):**

   ```bash
   cd backend && uvicorn main:app --reload --port 8000
   ```

   **Terminal 2 (Frontend):**

   ```bash
   npm run start:electron
   ```

4. **Production Build**

   ```bash
   # 1. Build the Python executable (via PyInstaller)
   npm run build:backend

   # 2. Build the Angular app (Tailwind v4 + Material) & Electron package
   npm run build:frontend
   npm run package
   ```

## 📈 Roadmap

- [ ] **Excel Ingestion:** Multi-sheet processing via Polars `read_excel`.
- [ ] **NLP Categorization:** Using **spaCy** to label unstructured revenue line-items.
- [ ] **Material 3 UI:** Dynamic forecasting dashboards with **Angular Material**.
- [ ] **Desktop Integration:** Native file system dialogs for secure spreadsheet selection.

---

**POPIC LLC Confidential** - _Internal Proof of Concept Phase_
