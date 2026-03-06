import httpx
from typing import List, Dict
from app.core.config import settings

async def fetch_pdc_alerts() -> List[Dict]:
    """Fetch disaster alerts from PDC (stub implementation)"""
    # Note: PDC API requires authentication and specific access
    # This is a placeholder implementation
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{settings.PDC_API}/alerts", timeout=10)
            if response.status_code == 200:
                return response.json()
    except Exception as e:
        print(f"PDC API error: {e}")
    return []