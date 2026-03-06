from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from datetime import datetime
from typing import List, Dict
from app.core.config import settings

def get_google_auth_flow():
    """Create Google OAuth flow"""
    return Flow.from_client_config(
        {
            "web": {
                "client_id": settings.GOOGLE_CLIENT_ID,
                "client_secret": settings.GOOGLE_CLIENT_SECRET,
                "redirect_uris": [settings.GOOGLE_REDIRECT_URI],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        },
        scopes=["https://www.googleapis.com/auth/calendar.readonly"],
    )

async def get_calendar_events(credentials_dict: dict, start_date: str, end_date: str) -> List[Dict]:
    """Fetch user's calendar events"""
    credentials = Credentials(**credentials_dict)
    service = build("calendar", "v3", credentials=credentials)
    
    events_result = service.events().list(
        calendarId="primary",
        timeMin=start_date,
        timeMax=end_date,
        singleEvents=True,
        orderBy="startTime",
    ).execute()
    
    return events_result.get("items", [])

def extract_location_from_event(event: Dict) -> str:
    """Extract location from calendar event"""
    return event.get("location", "")