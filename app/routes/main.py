import io

from fastapi import APIRouter, Request, HTTPException, UploadFile, File
from fastapi.templating import Jinja2Templates
from datetime import date
from starlette.responses import HTMLResponse, StreamingResponse
from app.services.meteo_service import MeteoService
from app.services.pse_service import GenerationService
from app.services.forecast_service import train_model, predict_future

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

# global cache
LAST_DF = None

@router.get("/")
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@router.get("/fetch_meteo", response_class=HTMLResponse)
def fetch_meteo(request: Request, start_date: date, end_date: date, latitude: float, longitude: float):
    if start_date > end_date:
        raise HTTPException(
                status_code=400,
                detail="start date nie może być późniejsza niż end date"
                )

    global LAST_DF

    try:
        result = MeteoService().fetch_history(start_date, end_date, latitude, longitude)
        result.save_to_db()
        LAST_DF = result.dataframe
        result_html = result.dataframe.to_html(classes="table table-striped", index=False)

        return templates.TemplateResponse(
            "result_table.html",
            {
                "request": request,
                "result_html": result_html
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f'Błąd podczas pobierania danych historcyznych z Open Meteo: {e}'
        )


@router.get("/fetch_meteo_forecast", response_class=HTMLResponse)
def fetch_meteo_forecast(request: Request, latitude: float, longitude: float):

    global LAST_DF

    try:
        result = MeteoService().fetch_forecast(latitude, longitude)
        result.save_to_db()
        LAST_DF = result.dataframe
        result_html = result.dataframe.to_html(classes="table table-striped", index=False)

        return templates.TemplateResponse(
            "result_table.html",
            {
                "request": request,
                "result_html": result_html
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f'Błąd podczas pobierania prognozy z Open Meteo: {e}'
        )

@router.get("/fetch_generation", response_class=HTMLResponse)
def fetch_generation(request: Request, start_date: date, end_date: date):

    global LAST_DF

    if start_date > end_date:
        raise HTTPException(
                status_code=400,
                detail="start date nie może być późniejsza niż end date"
                )

    try:
        result = GenerationService().fetch_data(start_date, end_date)
        result.save_to_db()
        LAST_DF = result.dataframe
        result_html = result.dataframe.to_html(classes="table table-striped", index=False)

        return templates.TemplateResponse(
            "result_table.html",
            {
                "request": request,
                "result_html": result_html
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f'Błąd poczas pobierania danych z PSE: {e}'
        )


@router.get("/create_forecast", response_class=HTMLResponse)
def create_forecast(request: Request, model_name: str = 'RandomForestRegressor', steps: int = 24, n_lags: int = 24):

    global LAST_DF

    try:
        generation_df = GenerationService().load_from_db().set_index_in_df().prepare_to_forecast().dataframe
        meteo_df = MeteoService().load_from_db().set_index_in_df().prepare_to_forecast().dataframe

        joined_df = generation_df.join(meteo_df, how="inner")
        meteo_forecast_df = meteo_df[~meteo_df.index.isin(generation_df.index)]

        train_model(joined_df, "fcst_pv_tot_gen", model_name=model_name, n_lags=n_lags)

        result = predict_future(joined_df, meteo_forecast_df, "fcst_pv_tot_gen", steps=steps)
        LAST_DF = result

        result_html = result.to_html(classes="table table-striped", index=False)

        return templates.TemplateResponse(
            "result_table.html",
            {
                "request": request,
                "result_html": result_html
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f'Błąd poczas wyznaczania prognozy {e}'
        )


@router.post("/upload", response_class=HTMLResponse)
async def upload_file(request: Request ,file: UploadFile = File(...)):

    global LAST_DF

    try:
        content = await file.read()
        result = GenerationService().load_from_file(file.filename, content)
        result.save_to_db()
        LAST_DF = result.dataframe

        result_html = result.dataframe.to_html(classes="table table-striped", index=False)
        return templates.TemplateResponse(
            "result_table.html",
            {
                "request": request,
                "result_html": result_html
            }
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Błąd podczas wczytywania pliku: {e}")

@router.get("/get_agg_data", response_class=HTMLResponse)
def get_generation_data(request: Request, table: str):

    global LAST_DF

    try:
        if table == "generation":
            result = GenerationService().get_dates_list()
        elif table == "meteo":
            result = MeteoService().get_dates_list()

        LAST_DF = result

        result_html = result.to_html(classes="table table-striped", index=False)

        return templates.TemplateResponse(
            "result_table.html",
            {
                "request": request,
                "result_html": result_html
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f'Błąd podczas pobierania danych z bazy: {e}'
        )

@router.get("/download-csv")
def download_csv():
    global LAST_DF

    if LAST_DF is None:
        return {"ERROR:" "Brak danych do zapisania"}

    buffer = io.BytesIO()
    LAST_DF.to_csv(buffer, index=False)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content=Disposition": "attachment; filename=data.csv"},
    )
