import uvicorn
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware

from ingestion.engine import consolidate_excel_data

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
async def analyze_file(file: UploadFile = File(...), active_tab: str = Form(...)):

    print("@data of Active Tab: " + active_tab)

    contents = await file.read()
    
    # Initialize variables to avoid UnboundLocalError
    final_list = []
    column_names = []

    if active_tab != "salesforce":
        # Process regular excel
        df = pl.read_excel(io.BytesIO(contents))
        df = df.fill_nan(None)
        column_names = df.columns
        final_list = df.to_dicts()
    else:
        # Process specialized Salesforce consolidation
        # This returns a list of dicts directly
        final_list = consolidate_excel_data(contents)
        
        # Get columns from the first dictionary in the list if data exists
        if len(final_list) > 0:
            column_names = list(final_list[0].keys())

    # Calculate the count once
    total_count = len(final_list)
    
    return {
        "filename": file.filename,
        "total_rows": total_count,
        "columns": column_names,
        "data": final_list  # This is the actual data content
    }

if __name__ == "__main__":
    # Important: Bind to 127.0.0.1 to avoid firewall popups
    uvicorn.run(app, host="127.0.0.1", port=8000)