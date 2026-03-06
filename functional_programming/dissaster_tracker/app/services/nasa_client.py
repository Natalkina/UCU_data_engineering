import httpx
from typing import List, Dict
from app.core.config import settings

async def fetch_nasa_events() -> List[Dict]:
    """Fetch disaster events from NASA EONET API"""
    async with httpx.AsyncClient() as client:
        response = await client.get(settings.NASA_EONET_API)
        response.raise_for_status()
        data = response.json()
        return data.get("events", [])

async def fetch_nasa_events_by_category(category: str) -> List[Dict]:
    """Fetch events filtered by category"""
    url = f"{settings.NASA_EONET_API}?category={category}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("events", [])

async def fetch_nasa_events_by_date(start_date: str, end_date: str = None) -> List[Dict]:
    """Fetch events within date range"""
    url = f"{settings.NASA_EONET_API}?start={start_date}"
    if end_date:
        url += f"&end={end_date}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("events", [])