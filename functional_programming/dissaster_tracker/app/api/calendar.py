from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from app.api.auth import oauth2_scheme
from app.services import google_auth, nasa_client
from app.core.security import decode_token
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from app.core.config import settings

router = APIRouter(prefix="/calendar", tags=["calendar"])

class GoogleAuthCode(BaseModel):
    code: str

class DisasterWarning(BaseModel):
    event_title: str
    event_location: str
    event_date: str
    disaster_type: str
    disaster_distance_km: float
    warning_level: str

@router.post("/connect-google")
async def connect_google_calendar(auth_code: GoogleAuthCode, token: str = Depends(oauth2_scheme)):
    """Connect user's Google Calendar"""
    try:
        payload = decode_token(token)
        email = payload.get("sub")
        
        flow = google_auth.get_google_auth_flow()
        flow.fetch_token(code=auth_code.code)
        credentials = flow.credentials
        
        # Store credentials (in production, encrypt and store in database)
        from app.api.auth import users_db
        if email in users_db:
            users_db[email]["google_credentials"] = {
                "token": credentials.token,
                "refresh_token": credentials.refresh_token,
                "token_uri": credentials.token_uri,
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret,
            }
        
        return {"message": "Google Calendar connected successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/events")
async def get_calendar_events(
    start_date: str,
    end_date: str,
    token: str = Depends(oauth2_scheme)
):
    """Get user's calendar events"""
    try:
        payload = decode_token(token)
        email = payload.get("sub")
        
        from app.api.auth import users_db
        user = users_db.get(email)
        if not user or not user.get("google_credentials"):
            raise HTTPException(status_code=400, detail="Google Calendar not connected")
        
        events = await google_auth.get_calendar_events(
            user["google_credentials"],
            start_date,
            end_date
        )
        
        return {"events": events, "count": len(events)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/check-disasters", response_model=List[DisasterWarning])
async def check_disasters_for_events(
    start_date: str,
    end_date: str,
    token: str = Depends(oauth2_scheme)
):
    """Check if calendar events are near disaster zones"""
    try:
        payload = decode_token(token)
        email = payload.get("sub")
        
        from app.api.auth import users_db
        user = users_db.get(email)
        if not user or not user.get("google_credentials"):
            raise HTTPException(status_code=400, detail="Google Calendar not connected")
        
        # Get calendar events
        calendar_events = await google_auth.get_calendar_events(
            user["google_credentials"],
            start_date,
            end_date
        )
        
        # Get disasters
        disasters = await nasa_client.fetch_nasa_events_by_date(start_date, end_date)
        
        warnings = []
        geolocator = Nominatim(user_agent="disaster_tracker")
        
        for event in calendar_events:
            location = google_auth.extract_location_from_event(event)
            if not location:
                continue
            
            try:
                event_geo = geolocator.geocode(location)
                if not event_geo:
                    continue
                
                event_coords = (event_geo.latitude, event_geo.longitude)
                
                for disaster in disasters:
                    for geometry in disaster.get("geometry", []):
                        coords = geometry.get("coordinates", [])
                        if len(coords) >= 2:
                            disaster_coords = (coords[1], coords[0])
                            distance = geodesic(event_coords, disaster_coords).km
                            
                            if distance <= settings.DISASTER_RADIUS_KM:
                                warning_level = "HIGH" if distance < 50 else "MEDIUM"
                                warnings.append(DisasterWarning(
                                    event_title=event.get("summary", ""),
                                    event_location=location,
                                    event_date=event.get("start", {}).get("dateTime", ""),
                                    disaster_type=disaster.get("title", ""),
                                    disaster_distance_km=round(distance, 2),
                                    warning_level=warning_level
                                ))
            except Exception as e:
                print(f"Error processing event: {e}")
                continue
        
        return warnings
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))