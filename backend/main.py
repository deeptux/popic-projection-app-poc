import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

from ingestion.engine import consolidate_excel_data, ingest_salesforce, merge_rlip_rap
from ingestion.commission import ingest_commission
from ingestion.referral import ingest_referral
from analytics.charts import (
    top_additional_rent_line,
    top_total_available_units_bar,
    pie_popic_fee_rlip,
    pie_popic_fee_rap,
    pie_popic_fee_comparison,
    commission_monthly_commission_line,
    commission_monthly_pnl_bar,
)

import io
import polars as pl

AnalyticsBody = dict  # {"data": list[dict], "columns": list[str]}


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
        try:
            result = ingest_salesforce(contents, filename=file.filename or None)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Upload a valid Salesforce Captive Report file. The file {file.filename or 'unknown'} is invalid.",
            )
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


@app.post("/upload/commission-report/basic")
async def Upload_CommissionReportBasic(file: UploadFile = File(...)):
    """Raw commission report: read Excel and return data/columns without ETL."""
    contents = await file.read()
    df = pl.read_excel(io.BytesIO(contents))
    df = df.fill_nan(None)
    return {
        "filename": file.filename,
        "total_rows": len(df),
        "columns": df.columns,
        "data": df.to_dicts(),
    }


@app.post("/upload/commission-report")
async def Upload_CommissionReport(file: UploadFile = File(...)):
    """Cleaned commission report: run ETL (group by Salesperson, Captive, Client; filter subtotals)."""
    contents = await file.read()
    try:
        result = ingest_commission(contents, filename=file.filename or None)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Upload a valid Commission Report file. The file {file.filename or 'unknown'} is invalid.",
        )
    data = result["data"]
    columns = result.get("columns", list(data[0].keys()) if data else [])
    return {
        "filename": file.filename,
        "total_rows": len(data),
        "columns": columns,
        "data": data,
        "ingestion_metadata": result.get("ingestion_metadata", {}),
    }


@app.post("/upload/referral-report/basic")
async def Upload_ReferralReportBasic(file: UploadFile = File(...)):
    """Raw referral report: read Excel and return data/columns without ETL."""
    contents = await file.read()
    df = pl.read_excel(io.BytesIO(contents))
    df = df.fill_nan(None)
    return {
        "filename": file.filename,
        "total_rows": len(df),
        "columns": df.columns,
        "data": df.to_dicts(),
    }


@app.post("/upload/referral-report")
async def Upload_ReferralReport(file: UploadFile = File(...)):
    """Cleaned referral report: run ETL (group by Vendor, Captive, Client; filter subtotals)."""
    contents = await file.read()
    try:
        result = ingest_referral(contents, filename=file.filename or None)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Upload a valid Referral Report file. The file {file.filename or 'unknown'} is invalid.",
        )
    data = result["data"]
    columns = result.get("columns", list(data[0].keys()) if data else [])
    return {
        "filename": file.filename,
        "total_rows": len(data),
        "columns": columns,
        "data": data,
        "ingestion_metadata": result.get("ingestion_metadata", {}),
    }


def _analytics_payload(body: AnalyticsBody) -> tuple[list[dict], list[str]]:
    data = body.get("data") or []
    columns = body.get("columns") or []
    if not isinstance(data, list) or not isinstance(columns, list):
        raise HTTPException(status_code=400, detail="Request body must have 'data' (array) and 'columns' (array).")
    return data, columns


@app.post("/analytics/top-additional-rent-line")
async def analytics_top_additional_rent_line(body: AnalyticsBody = Body(...)):
    """Top 7 entities (Captive or Captive+Client) by Additional Rent sum. For line chart."""
    data, columns = _analytics_payload(body)
    result = top_additional_rent_line(data, columns)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.post("/analytics/top-total-available-units-bar")
async def analytics_top_total_available_units_bar(body: AnalyticsBody = Body(...)):
    """Top 5 entities by Total Available Units (one per entity). For bar chart."""
    data, columns = _analytics_payload(body)
    result = top_total_available_units_bar(data, columns)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.post("/analytics/pie-popic-fee-rlip")
async def analytics_pie_popic_fee_rlip(body: AnalyticsBody = Body(...)):
    """Top 4 entities by POPIC Fee RLIP sum + Others. For pie chart."""
    data, columns = _analytics_payload(body)
    result = pie_popic_fee_rlip(data, columns)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.post("/analytics/pie-popic-fee-rap")
async def analytics_pie_popic_fee_rap(body: AnalyticsBody = Body(...)):
    """Top 4 entities by POPIC Fee RAP sum + Others. For pie chart."""
    data, columns = _analytics_payload(body)
    result = pie_popic_fee_rap(data, columns)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.post("/analytics/pie-popic-fee-comparison")
async def analytics_pie_popic_fee_comparison(body: AnalyticsBody = Body(...)):
    """Two slices: total POPIC Fee RLIP vs total POPIC Fee RAP (percentage of combined)."""
    data, columns = _analytics_payload(body)
    result = pie_popic_fee_comparison(data, columns)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.post("/analytics/commission-monthly-commission-line")
async def analytics_commission_monthly_commission_line(body: AnalyticsBody = Body(...)):
    """Monthly totals for January Commission–December Commission. For line chart."""
    data, columns = _analytics_payload(body)
    result = commission_monthly_commission_line(data, columns)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@app.post("/analytics/commission-monthly-pnl-bar")
async def analytics_commission_monthly_pnl_bar(body: AnalyticsBody = Body(...)):
    """Monthly totals for January P&L–December P&L. For bar chart."""
    data, columns = _analytics_payload(body)
    result = commission_monthly_pnl_bar(data, columns)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


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