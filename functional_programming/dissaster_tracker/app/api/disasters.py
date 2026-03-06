from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime
from app.services import nasa_client, pdc_client
from geopy.distance import geodesic

router = APIRouter(prefix="/disasters", tags=["disasters"])

@router.get("/events")
async def get_all_disasters():
    """Get all disaster events from NASA EONET"""
    try:
        nasa_events = await nasa_client.fetch_nasa_events()
        pdc_events = await pdc_client.fetch_pdc_alerts()
        return {"nasa": nasa_events, "pdc": pdc_events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/events/by-date")
async def get_disasters_by_date(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """Get disasters within date range"""
    try:
        events = await nasa_client.fetch_nasa_events_by_date(start_date, end_date)
        return {"events": events, "count": len(events)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/events/by-location")
async def get_disasters_near_location(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius_km: float = Query(100, description="Radius in kilometers")
):
    """Get disasters near a specific location"""
    try:
        all_events = await nasa_client.fetch_nasa_events()
        nearby_events = []
        
        for event in all_events:
            for geometry in event.get("geometry", []):
                coords = geometry.get("coordinates", [])
                if len(coords) >= 2:
                    event_lat, event_lon = coords[1], coords[0]
                    distance = geodesic((lat, lon), (event_lat, event_lon)).km
                    if distance <= radius_km:
                        nearby_events.append({**event, "distance_km": distance})
                        break
        
        return {"events": nearby_events, "count": len(nearby_events)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hotspots")
async def get_disaster_hotspots(limit: int = Query(10, description="Number of hotspots")):
    """Get disaster hotspots - areas with most disasters"""
    try:
        events = await nasa_client.fetch_nasa_events()
        location_counts = {}
        
        for event in events:
            for geometry in event.get("geometry", []):
                coords = geometry.get("coordinates", [])
                if len(coords) >= 2:
                    location_key = f"{coords[1]:.1f},{coords[0]:.1f}"
                    location_counts[location_key] = location_counts.get(location_key, 0) + 1
        
        hotspots = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:limit]
        return {"hotspots": [{"location": k, "count": v} for k, v in hotspots]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))