# ⚙️ POPIC Revenue Engine (Backend)

This is the analytical core of the POPIC LLC Revenue Forecasting application. It is a high-performance Python service that handles heavy-duty data processing, NLP-based categorization, and revenue projection logic.

## 🚀 Core Technologies

- **Python 3.12:** Leveraging the latest performance optimizations and type-hinting.
- **FastAPI:** High-speed asynchronous API layer.
- **Polars:** Rust-based DataFrame library used for O(1) speed spreadsheet ingestion.
- **spaCy:** Natural Language Processing for entity recognition and line-item classification.
- **PyInstaller:** Used to bundle this backend into a standalone binary for Electron.

---

## 📂 Internal Structure

```text
/backend
├── /api                 # FastAPI route definitions
│   ├── routes.py        # Main API endpoints (Revenue, Upload, Export)
│   └── health.py        # Heartbeat endpoint for Electron startup
├── /engine              # The "Brain" of the app
│   ├── processor.py     # Polars logic for Excel/CSV manipulation
│   ├── nlp_models.py    # spaCy pipeline configurations
│   └── forecaster.py    # Revenue projection algorithms
├── main.py              # Entry point & Uvicorn runner
├── requirements.txt     # Python dependencies
└── backend.spec         # PyInstaller configuration for executable bundling
```

## 🛠️ Local Development

### 1. Environment Setup

It is strictly recommended to use **Python 3.12**.

```bash
 # Create the virtual environment
`"C:\**CHANGE_THIS[Users\Path]**\Programs\Python\Python312\python.exe" -m venv pyenv12`

 # Activate the virtual environment
 # On macOS/Linux:
 source pyenv12/bin/activate
 # On Windows:
 .\pyenv12\Scripts\activate

 # Install dependencies
 pip install -r requirements.txt -y

 # Download spaCy model
 python -m spacy download en_core_web_sm
```

### 2. Running the Server

The API documentation will be available at: http://localhost:8000/docs

```bash
 uvicorn main:app --reload --port 8000
```

## 📦 Bundling for Electron

To allow the Electron frontend to launch this backend, it must be compiled into a single executable binary.

### Build Executable

**Note**: The compiled binary will be located in /backend/dist/. The Electron main process is configured to look for this binary during the "Safe-Start" lifecycle.

```bash
 # Ensure you are in the /backend directory with venv active
 pyinstaller --noconfirm --onefile --windowed --name "popic-engine" \
 --collect-all spacy \
 --collect-all en_core_web_sm \
 main.py
```

## 🛣️ API Endpoints (Summary)

| Method   | Endpoint        | Description                                                                            |
| :------- | :-------------- | :------------------------------------------------------------------------------------- |
| **GET**  | `/health`       | Heartbeat check used by Electron on startup to verify backend readiness.               |
| **POST** | `/upload`       | Ingests Excel files using **Polars** for ultra-fast data parsing and validation.       |
| **GET**  | `/forecast`     | Executes predictive algorithms to return revenue projections based on historical data. |
| **POST** | `/nlp/classify` | Leverages **spaCy** NLP to intelligently categorize unstructured revenue line items.   |

---

**POPIC LLC** | _Confidential Technical Documentation_
