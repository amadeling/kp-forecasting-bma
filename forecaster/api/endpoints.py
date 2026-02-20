

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