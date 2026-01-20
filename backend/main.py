import uvicorn
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware

import io
import polars as pl


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def test():
    return "Hello POPIC LLC Projection PoC App"

@app.post("/analyze")
async def analyze_file(file: UploadFile = File(...)):
    contents = await file.read()

    df = pl.read_excel(io.BytesIO(contents))
    df = df.fill_nan(None)
    data = df.to_dicts()

    # Return only the first 100 rows for the UI preview
    preview_data = df.head(100).to_dicts()
    
    return {
        "filename": file.filename,
        "total_rows": len(df),
        "columns": df.columns,
        "data": data
    }

if __name__ == "__main__":
    # Important: Bind to 127.0.0.1 to avoid firewall popups
    uvicorn.run(app, host="127.0.0.1", port=8000)