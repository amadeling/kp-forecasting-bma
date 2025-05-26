from fastapi import APIRouter, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from .tasks import process_csv_task
from .db import (
    validate_and_append_to_db, get_data, append_forecast_results, get_history_forecast,
    get_train_data_by_product_id, get_forecast_history_by_id, get_all_data,
    preprocess_uploaded_file  # <-- import the new function
)
import os
from datetime import date, timedelta
from pydantic import BaseModel
import pandas as pd
from io import StringIO

# Directory to save uploaded and processed files
UPLOAD_DIR = "uploads"
OUTPUT_DIR = "output"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

router = APIRouter()

class ForecastRow(BaseModel):
    TANGGAL: date
    TOTAL_JUMLAH: float

class StoreForecast(BaseModel):
    product_id: str
    results: list[ForecastRow]

@router.post("/internal/store-forecast")
async def store_forecast(payload: StoreForecast):
    """
    Internal endpoint: Celery calls this to persist forecast results.
    """
    try:
        append_forecast_results(payload.product_id, payload.results)
    except Exception as e:
        # Log error if needed, but don't print to stdout in production
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok"}

@router.post("/upload/")
async def upload_train_csv(
    file: UploadFile = UploadFile(...)
):
    """
    Endpoint to upload a train CSV file and start processing it asynchronously.
    """
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())

    # Preprocess the uploaded file (Excel/CSV) before validation/append
    try:
        processed_path = preprocess_uploaded_file(file_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Preprocessing failed: {e}")

    # Validate and append data to DuckDB
    try:
        validate_and_append_to_db(processed_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return JSONResponse(
        {"message": "File uploaded and processed successfully."},
        status_code=200
    )

# Endpoint to trigger CSV processing
@router.post("/process-csv/")
async def process_csv(
    target_product_id: str,
    start_date: date = None,
    end_date: date = None
):
    """
    Endpoint to process the uploaded CSV file asynchronously.
    """
    df = get_data(target_product_id)
    df = df[df["TANGGAL"] <= pd.to_datetime(start_date)]

    file_path = os.path.join(UPLOAD_DIR, "train.csv")
    df.to_csv(file_path, index=False)

    # Convert date to string for Celery serialization
    start_date_str = start_date.isoformat() if start_date else None
    end_date_str = end_date.isoformat() if end_date else None

    future_step = (end_date - df["TANGGAL"].max().date()).days + 1 if start_date and end_date else 0
    task = process_csv_task.apply_async(
        args=[file_path, target_product_id, future_step, start_date_str, end_date_str],
        countdown=5
    )

    if not task:
        raise HTTPException(status_code=500, detail="Task submission failed.")

    return JSONResponse(
        {"message": "CSV processing started.", "task_id": task.id},
        status_code=202
    )

# Endpoint to check status task
@router.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """
    Endpoint to check the status of a Celery task.
    """
    task = process_csv_task.AsyncResult(task_id)
    if task.state == "PENDING":
        response = {
            "state": task.state,
            "status": "Pending..."
        }
    elif task.state != "FAILURE":
        response = {
            "state": task.state,
            "result": task.result
        }
    else:
        response = {
            "state": task.state,
            "error": str(task.info)  # This will be the exception raised
        }
    
    return JSONResponse(response)

@router.get("/forecast-history/")
def get_forecast_history():
    """
    Endpoint to get the forecast history.
    """
    try:
        history = get_history_forecast()
        return JSONResponse(history)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/forecast/{product_id}")
def get_forecast(
        product_id: str,
    ):
    """
    Endpoint to get the forecast history for a specific product ID.
    """
    try:
        history = get_history_forecast(as_dict=True)
        filtered_history = [entry for entry in history if entry["product_id"] == product_id]
        if not filtered_history:
            raise HTTPException(status_code=404, detail="No forecast data found for the specified product ID.")
        return JSONResponse(filtered_history)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# get train data
@router.get("/train-data/{product_id}")
def get_train_data(
    product_id: str,
    start_date: date,
    end_date: date
):
    """
    Endpoint to get the train data for a specific product ID.
    """
    try:
        res = get_train_data_by_product_id(start_date, end_date, product_id)
        if len(res) == 0:
            raise HTTPException(status_code=404, detail="No train data found.")
        return JSONResponse(res)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/download/{filename}")
async def download_file(filename: str):
    """
    Endpoint to download a file.
    """
    file_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    
    return FileResponse(file_path, media_type='application/octet-stream', filename=filename)

@router.get("/forecast-history/{forecast_id}")
async def get_forecast_by_id(forecast_id: int):
    """
    Endpoint to get forecast results by ID.
    """
    try:
        data = get_forecast_history_by_id(forecast_id)

        return JSONResponse(data)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/all-train-data")
async def get_all_train_data():
    """
    Endpoint to get all train data.
    Stream as CSV for better performance.
    """
    try:
        df = get_all_data()

        def iter_csv():
            yield df.to_csv(index=False)

        return StreamingResponse(
            iter_csv(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=train_data.csv"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))