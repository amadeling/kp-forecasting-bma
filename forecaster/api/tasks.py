import os, requests
from celery import Celery
from kp_forecaster.pipeline import run_bma_pipeline

redis_host = os.getenv("REDIS_HOST", "localhost")
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")

celery = Celery(
    "tasks",
    broker=f"redis://{redis_host}:6379/0",
    backend=f"redis://{redis_host}:6379/0"
)

@celery.task
def process_csv_task(filepath: str, target_product_id: str, future_step: int = 0, start_date: str = None, end_date: str = None):
    if future_step <= 0:
        # If future_step is not provided or is less than or equal to 0, set it to 1
        future_step = 365
    
    results = run_bma_pipeline(filepath, target_product_id, future_step=future_step)
    if not results:
        return {"status": "failed"}

    future_df = results["future_forecast"]

    # Filter date range if start_date and end_date are provided
    if start_date and end_date:
        future_df = future_df[(future_df["TANGGAL"] >= start_date) & (future_df["TANGGAL"] <= end_date)]

    # Convert TANGGAL (Timestamp) to ISO‐formatted string for JSON serialization
    future_df["TANGGAL"] = future_df["TANGGAL"].dt.strftime("%Y-%m-%d")

    payload = {
        "product_id": target_product_id,
        "results": future_df.to_dict(orient="records")
    }

    print(f"payload: {payload}")

    resp = requests.post(f"{FASTAPI_URL}/internal/store-forecast", json=payload)
    resp.raise_for_status()
    return resp.json()

