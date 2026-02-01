from datetime import date
from pathlib import Path
import pandas as pd
from sqlalchemy import MetaData, Table, select, func
from app.core.db import make_engine
from app.core.fetcher import fetch_data_meteo
from config.config import MeteoServiceConfig



class MeteoService:

    def __init__(self):
        self.config = MeteoServiceConfig()
        self.dataframe = pd.DataFrame()


    def set_index_in_df(self):
        self.dataframe[self.config.df_index] = pd.to_datetime(self.dataframe[self.config.df_index])
        self.dataframe.set_index(self.config.df_index, inplace=True)
        self.dataframe[self.config.parquet_partition_col] = self.dataframe.index.date

        return self


    def fetch_history(self, start_date: date, end_date: date, latitude: float, longitude: float):
        self.dataframe = fetch_data_meteo(latitude, longitude, self.config.fetch_select, start_date, end_date)
        self.set_index_in_df()

        return self


    def fetch_forecast(self, latitude: float, longitude: float):
        self.dataframe = fetch_data_meteo(latitude, longitude, self.config.fetch_select)
        self.set_index_in_df()

        return self


    def save_to_parquet(self):

        output_path = Path(self.config.parquet_output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.dataframe.to_parquet(output_path, partition_cols=self.config.parquet_partition_col)

        return self


    def save_to_db(self):
        engine = make_engine(self.config.db_path)
        self.dataframe.to_sql(name=self.config.db_table,
                              con=engine,
                              if_exists="append",
                              index=True,
                              method="multi")

        return self


    def show_head(self):
        return {
            "columns": self.dataframe.columns.tolist(),
            "preview:": self.dataframe.head().to_dict(orient="records")
        }


    def load_from_db(self, history=True):
        engine = make_engine(self.config.db_path)
        metadata = MetaData()
        try:
            table = Table(self.config.db_table, metadata, autoload_with=engine)
        except Exception as e:
            raise RuntimeError(f"Failed to load table '{self.config.db_table}': {e}") from e
        stmt = select(table)
        with engine.connect() as conn:
            result = conn.execute(stmt)
            rows = result.fetchall()
            cols = result.keys()
            self.dataframe = pd.DataFrame(rows, columns=cols)

        return self


    def prepare_to_forecast(self):
        self.dataframe.drop([self.config.parquet_partition_col], axis=1, inplace=True)

        return self

    def get_dates_list(self):
        engine = make_engine(self.config.db_path)
        metadata = MetaData()
        try:
            table = Table(self.config.db_table, metadata, autoload_with=engine)
        except Exception as e:
            raise RuntimeError(f"Failed to reflect table '{self.config.db_table}': {e}") from e

        stmt = (
            select(
                table.c.business_date,
                func.count().label("cnt")
            )
            .group_by(table.c.business_date)
        )

        with engine.connect() as conn:
            result = conn.execute(stmt).fetchall()

        return pd.DataFrame(result, columns=["business_date", "cnt"])