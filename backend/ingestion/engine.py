import polars as pl
import io

def consolidate_excel_data(contents: bytes) -> list[dict]:
    """
    Reads an Excel file, cleans it, and consolidates rows by summing numeric fields 
    for each Client/Captive combo.
    """
    # 1. Read Excel File
    df = pl.read_excel(source=io.BytesIO(contents), infer_schema_length=10000)

    # 2. Clean Column Names
    new_cols = {col: col.replace(" ↑", "").strip() for col in df.columns}
    df = df.rename(new_cols)

    captive_col = "Captive Name: Captive Name"
    client_col = "Captive Name: Client"

    if captive_col not in df.columns or client_col not in df.columns:
        raise ValueError(f"Missing required columns. Found: {df.columns}")

    # 3. Handle Grouping & Filtering
    # Forward fill Captive Name to handle implicit groupings (empty cells under the first one)
    df = df.with_columns(pl.col(captive_col).fill_null(strategy="forward"))

    # Filter out 'Subtotal' rows and any remaining null captives
    df = df.filter(
        pl.col(captive_col).is_not_null() & 
        (pl.col(captive_col) != "Subtotal")
    )

    # 4. Handle "0" Clients
    # Cast Client to String so "0" is treated as the text "0", not a number/null.
    df = df.with_columns(
        pl.col(client_col).cast(pl.String).fill_null("")
    )

    # 5. Clean and Sum Numeric Columns
    target_columns = [
        "Additional Rent", "Additional Rent Charge", 
        "Retained At Property", "Retained at Property",
        "Gross Written Premium", "Taxes", "Credit Card Fees", 
        "Administrative Fees", "Net Premium to Captive", 
        "Claims Reserves", "Operating Expenses", "Proxy Tax", 
        "Other Expenses", "Net Income", "Total Available Units", 
        "Enrolled Units", "POPIC Fee RLIP FOF", "POPIC Fee RAP FOF",
        "POPIC Fee RLIP", "POPIC Fee RAP"
    ]
    
    existing_sum_cols = [c for c in target_columns if c in df.columns]

    clean_exprs = []
    for col_name in existing_sum_cols:
        # Clean string currency formats like "(100)", "$100", "1,000"
        if df[col_name].dtype == pl.String:
            clean_exprs.append(
                pl.col(col_name)
                .str.replace(r"\((.*)\)", "-$1") # Handle (Values) as negatives
                .str.replace_all(r"[$,]", "")    # Remove $ and ,
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
                .fill_null(0.0)
                .alias(col_name)
            )
        else:
            # Just fill nulls if already numeric
            clean_exprs.append(pl.col(col_name).fill_null(0.0).alias(col_name))
            
    if clean_exprs:
        df = df.with_columns(clean_exprs)

    # 6. Group and Aggregate
    grouped_df = df.group_by([captive_col, client_col]).agg(
        [pl.col(c).sum() for c in existing_sum_cols]
    )

    # 7. Sort and Return
    grouped_df = grouped_df.sort(captive_col)
    
    return grouped_df.to_dicts()