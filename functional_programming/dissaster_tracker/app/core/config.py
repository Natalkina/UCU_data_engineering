from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://user:password@localhost/disaster_tracker"
    SECRET_KEY: str = "your-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    NASA_EONET_API: str = "https://eonet.gsfc.nasa.gov/api/v3/events"
    PDC_API: str = "https://disasteralert.pdc.org/disasteralert/api"
    
    REDIS_URL: str = "redis://localhost:6379/0"
    
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REDIRECT_URI: str = "http://localhost:8000/auth/google/callback"
    
    DISASTER_RADIUS_KM: float = 100.0
    
    class Config:
        env_file = ".env"

settings = Settings()