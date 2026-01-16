import uvicorn
from fastapi import FastAPI, UploadFile, File
import polars as pl
import io

app = FastAPI()

@app.get("/")
def test():
    return "Hello POPIC LLC Projection PoC App"