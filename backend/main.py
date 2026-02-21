import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from ingestion.engine import consolidate_excel_data, ingest_salesforce, merge_rlip_rap

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

@app.post("/upload/salesforce-captive-summary/basic")
async def Upload_SalesforceCaptiveSummaryBasic(file: UploadFile = File(...)):
    contents = await file.read()

    df = pl.read_excel(io.BytesIO(contents))
    df = df.fill_nan(None)
    data = df.to_dicts()

    return {
        "filename": file.filename,
        "total_rows": len(df),
        "columns": df.columns,
        "data": data
    } 

@app.post("/upload/salesforce-captive-summary")
async def Upload_SalesforceCaptiveSummary(file: UploadFile = File(...), active_tab: str = Form(...)):

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
        # Process specialized Salesforce consolidation with metadata
        result = ingest_salesforce(contents, filename=file.filename or None)
        final_list = result["data"]
        ingestion_metadata = result["ingestion_metadata"]
        if len(final_list) > 0:
            column_names = list(final_list[0].keys())

    # Calculate the count once
    total_count = len(final_list)

    out = {
        "filename": file.filename,
        "total_rows": total_count,
        "columns": column_names,
        "data": final_list,
    }
    if active_tab == "salesforce":
        out["ingestion_metadata"] = result["ingestion_metadata"]
    return out


@app.post("/upload/salesforce-captive-summary/merge")
async def Upload_SalesforceCaptiveSummary_Merge_RLIP_RAP(
    file_rlip: UploadFile = File(..., description="RLIP-only Salesforce summary file"),
    file_rap: UploadFile = File(..., description="RAP-only Salesforce summary file"),
):
    """
    Merge separate RLIP-only and RAP-only Salesforce summary files into one table.
    Both files must have the same table-derived period (Month/Year). Returns same
    shape as single-file "/upload/salesforce-captive-summary (salesforce) plus ingestion_metadata with file_type='merged'.
    """
    contents_rlip = await file_rlip.read()
    contents_rap = await file_rap.read()
    try:
        result = merge_rlip_rap(
            contents_rlip,
            contents_rap,
            filename_rlip=file_rlip.filename,
            filename_rap=file_rap.filename,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    data = result["data"]
    columns = list(data[0].keys()) if data else []
    return {
        "filename": f"{file_rlip.filename or 'rlip'}+{file_rap.filename or 'rap'}",
        "total_rows": len(data),
        "columns": columns,
        "data": data,
        "ingestion_metadata": result["ingestion_metadata"],
    }


if __name__ == "__main__":
    # Important: Bind to 127.0.0.1 to avoid firewall popups
    uvicorn.run(app, host="127.0.0.1", port=8000)