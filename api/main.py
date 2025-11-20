from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field
from typing import List, Any, Dict
from datetime import date
import pandas as pd
import joblib


from fonc_get_flight_data import get_flight_data


# ==============================================================
# CONFIGURATION OF VARIABLES TYPES
# ==============================================================

CATEGORICAL_COLS = [
    "DS_AIRLINE_CODE",
    "DS_DEPARTURE_AIRPORT_CODE",
    "DS_ARRIVAL_AIRPORT_CODE",
]

NUMERIC_COLS = [
    "DS_DEPARTURE_AIRPORT_TEMP_CEL",
    "DS_DEPARTURE_AIRPORT_RAIN_MMHOUR",
    "DS_DEPARTURE_AIRPORT_WIND_KMH",
    "DS_DEPARTURE_AIRPORT_VIS_KM",
    "DS_ARRIVAL_AIRPORT_TEMP_CEL",
    "DS_ARRIVAL_AIRPORT_RAIN_MMHOUR",
    "DS_ARRIVAL_AIRPORT_WIND_KMH",
    "DS_ARRIVAL_AIRPORT_VIS_KM",
    "DS_PREV_DELAY_MIN",
    "DS_FLIGHT_DURATION_MIN",
    "DS_AIRLINE_RATING_NORM",
    "DS_DEPARTURE_AIRPORT_RATING_NORM",
    "DS_ARRIVAL_AIRPORT_RATING_NORM",
]

REQUIRED_COLUMNS = CATEGORICAL_COLS + NUMERIC_COLS
TARGET_COL = "DS_FINAL_DELAY_MIN"



# ==============================================================
# SCHEME PYDANTIC
# ==============================================================

class PredictionInput(BaseModel):
    # CATEGORIAL VARIABLES
    DS_AIRLINE_CODE: str = Field(..., example="AF")
    DS_DEPARTURE_AIRPORT_CODE: str = Field(..., example="CDG")
    DS_ARRIVAL_AIRPORT_CODE: str = Field(..., example="NCE")

    # NUMERIC VARIABLES
    DS_DEPARTURE_AIRPORT_TEMP_CEL: float = Field(..., example=15.5)
    DS_DEPARTURE_AIRPORT_RAIN_MMHOUR: float = Field(..., example=0.0)
    DS_DEPARTURE_AIRPORT_WIND_KMH: float = Field(..., example=12.0)
    DS_DEPARTURE_AIRPORT_VIS_KM: float = Field(..., example=10.0)

    DS_ARRIVAL_AIRPORT_TEMP_CEL: float = Field(..., example=18.2)
    DS_ARRIVAL_AIRPORT_RAIN_MMHOUR: float = Field(..., example=0.2)
    DS_ARRIVAL_AIRPORT_WIND_KMH: float = Field(..., example=8.0)
    DS_ARRIVAL_AIRPORT_VIS_KM: float = Field(..., example=9.5)

    DS_PREV_DELAY_MIN: float = Field(..., example=35)
    DS_FLIGHT_DURATION_MIN: float = Field(..., example=90)
    DS_AIRLINE_RATING_NORM: float = Field(..., example=0.75)
    DS_DEPARTURE_AIRPORT_RATING_NORM: float = Field(..., example=0.8)
    DS_ARRIVAL_AIRPORT_RATING_NORM: float = Field(..., example=0.85)

    model_config = ConfigDict(extra="allow")


class BatchPredictionInput(BaseModel):
    items: List[PredictionInput]


class PredictionOutput(BaseModel):
    predicted_delay_min: float


class FlightRequest(BaseModel):
    flight_number: str = Field(..., example="AF1234")
    flight_date: str = Field(..., example="30/10/25")


# ==============================================================
# API INITIALISATION
# ==============================================================

app = FastAPI(title="✈️ Flight delay prediction API", version="1.0")

# CORS : Activation
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# LOADING : Complete pipeline (OneHotEncoder + RandomForest)
try:
    model = joblib.load("flight_delay_pipeline.joblib")
except Exception as e:
    raise RuntimeError(f"Erreur lors du chargement du modèle : {e}") from e


# ==============================================================
# DATA CHECK/PREP
# ==============================================================

def validate_and_prepare(payloads: List[Dict[str, Any]]) -> pd.DataFrame:
    """Validate and prepare input data on dataframe format"""
    # CHECK : Missing columns
    missing = [c for c in REQUIRED_COLUMNS if any(c not in p for p in payloads)]
    if missing:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Colonnes manquantes",
                "missing_columns": missing,
                "required_columns": REQUIRED_COLUMNS,
            },
        )

    # CREATION : Of the dataFrame
    df = pd.DataFrame(payloads)
    df = df.reindex(columns=REQUIRED_COLUMNS)
    return df




# ==============================================================
# ENDPOINTS
# ==============================================================
from fastapi.responses import StreamingResponse
import json
import time

# CHECK ENDPOINT
@app.get("/health")
def health():
    """Check if API and model are operational"""
    return {"status": "ok", "model_loaded": True}

# INFO ENDPOINT
@app.get("/model-info")
def model_info():
    """Renvoie les infos sur le modèle et les features attendues"""
    return {
        "categorical_features": CATEGORICAL_COLS,
        "numeric_features": NUMERIC_COLS,
        "target_variable": TARGET_COL,
        "note": (
            "Les nouvelles catégories non vues pendant l’entraînement "
            "sont automatiquement gérées par handle_unknown='ignore'."
        ),
    }

# DEBUG/TEST ENDPOINT
@app.post("/predict", response_model=PredictionOutput)
def predict_one(data: PredictionInput):
    """Unique prediction"""
    df = validate_and_prepare([data.model_dump()])
    try:
        pred = model.predict(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur pendant la prédiction : {e}")
    return {"predicted_delay_min": float(pred[0])}

# MAIN ENDPOINT 
@app.post("/predict-flight")
def predict_flight_stream(request: FlightRequest):
    def generate():

        yield json.dumps({"step": "connexion_api", "status": "ok"}) + "\n"

        # DATA : Scraping + enrichment
        generator_yield = []
        features_dict, df_data_prov = get_flight_data(
            request.flight_number,
            request.flight_date,
            progress_callback=lambda s: generator_yield.append(
                json.dumps({"step": s, "status": "ok"}) + "\n"
            )
        )

        # STREAMLIT : Data transfer to Streamlit app
        dep_code = None
        arr_code = None
        airline_name = None
        departure_name = None
        arrival_name = None

        if df_data_prov is not None and not df_data_prov.empty:
            airline_name = df_data_prov.iloc[0].get("DS_AIRLINE_CODE")
            departure_name = df_data_prov.iloc[0].get("DS_DEPARTURE_AIRPORT")
            arrival_name = df_data_prov.iloc[0].get("DS_ARRIVAL_AIRPORT")
            arr_code = df_data_prov.iloc[0].get("DS_ARRIVAL_AIRPORT_CODE")
            dep_code = df_data_prov.iloc[0].get("DS_DEPARTURE_AIRPORT_CODE")

        yield json.dumps({
            "step": "scraping_fr24",
            "status": "ok",
            "DS_DEPARTURE_AIRPORT_CODE": dep_code,
            "DS_ARRIVAL_AIRPORT_CODE": arr_code,
            "DS_AIRLINE_CODE": airline_name,
            "DS_DEPARTURE_AIRPORT": departure_name,
            "DS_ARRIVAL_AIRPORT": arrival_name}) + "\n"


        # STREAMLIT : Forward progress in real time of all API steps
        for y in generator_yield:
            yield y

        # PREPARATION : Final preparation before prediction
        df = validate_and_prepare([features_dict])
        yield json.dumps({"step": "api_preparation", "status": "ok"}) + "\n"

        # PREDICTION
        pred = model.predict(df)
        delay = float(pred[0])
        yield json.dumps({
            "step": "prediction",
            "status": "done",
            "predicted_delay_min": delay
        }) + "\n"

    return StreamingResponse(generate(), media_type="application/json")
