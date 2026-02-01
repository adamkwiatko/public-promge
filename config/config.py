from pydantic import BaseModel
from typing import List

class FileServiceConfig(BaseModel):
    parquet_output_path: str = 'app/data/uploaded_file.parquet'
    required_columns: List[str] = ["dataczas", "wartosc"]
    db_path: str = 'app/data/app.db'

class MeteoServiceConfig(BaseModel):
    fetch_select: List[str] = ["temperature_2m", "cloud_cover", "wind_speed_10m", "wind_gusts_10m",
                               "relative_humidity_2m", "shortwave_radiation", "direct_radiation", "diffuse_radiation",
                               "global_tilted_irradiance", "direct_normal_irradiance", "terrestrial_radiation"
                            ]
    df_index: str = "time"
    parquet_output_path: str = 'app/data/meteo_data'
    parquet_partition_col: str = "business_date"
    db_path: str = 'sqlite:///app/data/app.db'
    db_table: str = 'meteo'

class PseServiceConfig(BaseModel):
    fetch_select: List[str] = ["plan_dtime", "fcst_pv_tot_gen"]
    df_index: str = "plan_dtime"
    parquet_output_path: str = 'app/data/pse_data'
    parquet_partition_col: str = "business_date"
    db_path: str = 'sqlite:///app/data/app.db'
    db_table: str = 'pse'
    file_columns: List[str] = ["plan_dtime", "fcst_pv_tot_gen"]