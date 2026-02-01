from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    API_URL_PSE: str
    API_URL_METEO_HIST: str
    API_URL_METEO_FRCST: str

    class Config:
        env_file = ".env"

settings = Settings()
