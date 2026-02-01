from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
import pandas as pd
import numpy as np

class CyclicalFeatures(BaseEstimator, TransformerMixin):
    def __init__(self, dt_columns, cycles):
        """
        dt_columns: lista kolumn datetime
        cycles: słownik cykli, np.:
            {
                "hour": 24,
                "weekday": 7,
                "month": 12,
                "dayofyear": "leap"   # specjalna wartość
            }
        """
        self.dt_columns = dt_columns
        self.cycles = cycles

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()

        for col in self.dt_columns:
            dt = X[col]

            for period, max_periods in self.cycles.items():

                # pobranie wartości okresu
                attr = getattr(dt.dt, period)
                values = attr() if callable(attr) else attr

                # obsługa roku przestępnego
                if period == "dayofyear" and max_periods == "leap":
                    # dynamiczny okres: 365 lub 366
                    year_lengths = dt.dt.is_leap_year.replace({True: 366, False: 365})
                    denom = year_lengths
                else:
                    denom = max_periods

                # nazwy kolumn
                base = f"{col}_{period}"

                # cechy cykliczne
                X[f"{base}_sin"] = np.sin(2 * np.pi * values / denom)
                X[f"{base}_cos"] = np.cos(2 * np.pi * values / denom)

        return X

def compute(generation, meteo_hist, meteo_forecast):

    #  add datetime for indexing production data
    generation["plan_dtime"] = pd.to_datetime(generation["plan_dtime"])
    generation.set_index("plan_dtime")

    #  add datetime for indexing weather data
    meteo_hist["time"] = pd.to_datetime(meteo_hist["time"])
    meteo_hist.set_index("time")

    meteo_forecast["time"] = pd.to_datetime(meteo_forecast["time"])
    meteo_forecast.set_index("time")

    #  join data
    joined = generation.join(meteo_hist, how="inner")

    #  prepare pipline
    cycles = {
        "hour": 24,
        "weekday": 7,
        "month": 12,
        "dayofyear": "leap"
    }
    transformer = CyclicalFeatures(dt_columns=["plan_dtime"], cycles=cycles)

    pipeline = Pipeline([
        ("cyclical", transformer),
        ("model", RandomForestRegressor())
        ])

    y = joined["target"]
    pipeline.fit(joined, y)

    y_pred = pipeline.predict(meteo_forecast)

    return y_pred
