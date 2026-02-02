from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np
import joblib
from importlib import import_module

class CyclicalFeatures(BaseEstimator, TransformerMixin):
    def __init__(self, pv_output, n_lags=24):

        self.pv_output = pv_output
        self.n_lags = n_lags

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        df = X.copy()

        df["dt_hour"] = df.index.hour
        df["dt_dayofyear"] = df.index.dayofyear
        df["dt_month"] = df.index.month

        df["dt_hour_sin"] = np.sin(2 * np.pi * df["dt_hour"] / 24)
        df["dt_hour_cos"] = np.cos(2 * np.pi * df["dt_hour"] / 24)
        df["dt_dayofyear_sin"] = np.sin(2 * np.pi * df["dt_dayofyear"] / 365)
        df["dt_dayofyear_cos"] = np.cos(2 * np.pi * df["dt_dayofyear"] / 365)
        df["dt_month_sin"] = np.sin(2 * np.pi * df["dt_month"] / 12)
        df["dt_month_cos"] = np.cos(2 * np.pi * df["dt_month"] / 12)

        for i in range(1, self.n_lags + 1):
            df[f"{self.pv_output}_lag_{i}"] = df[f"{self.pv_output}"].shift(i)

        lag_cols =[f"{self.pv_output}_lag_{i}" for i in range(1, self.n_lags + 1)]
        df = df.dropna(subset=lag_cols)

        if self.pv_output in df.columns:
            df= df.drop(columns=[self.pv_output])

        return df

def train_model(df: pd.DataFrame, pv_output: str, model_name: str, model_path: str = "pv_model.pkl", n_lags: int = 24):
    """
    Trenuje pipeline do prognozy PV i zapisuje go do pliku.
    df – DataFrame z kolumnami pogodowymi + pv_output
    """

    y = df[pv_output]
    X = df.copy()

    transformer = CyclicalFeatures(pv_output=pv_output, n_lags=n_lags)

    if model_name in ["LinearRegression", "Ridge", "Lasso"]:
        module = import_module("sklearn.linear_model")
    elif model_name in ["DecisionTreeRegressor"]:
        module = import_module("sklearn.tree")
    elif model_name in ["RandomForestRegressor", "GradientBoostingRegressor"]:
        module = import_module("sklearn.ensemble")
    elif model_name in ["SVR"]:
        module = import_module("sklearn.svm")
    elif model_name in ["KNeighborsRegressor"]:
        module = import_module("sklearn.neighbors")
    elif model_name in ["MLPRegressor"]:
        module = import_module("sklearn.neural_network")

    model = getattr(module, model_name)

    pipeline = Pipeline([
        ("features", transformer),
        ("scaler", StandardScaler()),
        ("model", model())
    ])

    X_trans = pipeline.named_steps["features"].transform(X)
    y_aligned = y.iloc[-len(X_trans):]

    pipeline.fit(X, y_aligned)

    joblib.dump(pipeline, model_path)

    return {"status": "trained", "model_path": model_path}

def predict_future(
    history_df: pd.DataFrame,
    future_weather: pd.DataFrame,
    pv_output: str,
    model_path: str = "pv_model.pkl",
    steps: int = 24
):
    """
    Generuje prognozę PV na podstawie historii i przyszłej pogody.
    history_df – ostatnie dane historyczne (z pv_output)
    future_weather – przyszłe dane pogodowe (bez pv_output)
    """

    pipeline = joblib.load(model_path)

    df = history_df.copy()
    preds = []

    for i in range(steps):
        row = future_weather.iloc[i:i+1].copy()
        row[pv_output] = df[pv_output].iloc[-1]  # placeholder

        # budujemy cechy tak jak w pipeline
        features = pipeline.named_steps["features"].transform(
            pd.concat([df.tail(24), row])
        ).tail(1)

        # skalowanie + predykcja
        X_scaled = pipeline.named_steps["scaler"].transform(features)
        y_pred = pipeline.named_steps["model"].predict(X_scaled)[0]

        preds.append(float(y_pred))

        # dodajemy predykcję do historii
        new_row = row.copy()
        new_row[pv_output] = y_pred
        df = pd.concat([df, new_row])

    future_index = future_weather.index[:steps]

    return pd.DataFrame({"plan_dtime": future_index, "pv_output": preds})
