from app.workers.celery_app import celery_app
from app.services import nasa_client
from app.api.auth import users_db
import asyncio

@celery_app.task
def check_disasters_for_all_users():
    """Periodic task to check disasters for all users"""
    asyncio.run(async_check_disasters())

async def async_check_disasters():
    """Check disasters for all users with connected calendars"""
    disasters = await nasa_client.fetch_nasa_events()
    
    for email, user in users_db.items():
        if user.get("google_credentials"):
            # Check user's calendar events against disasters
            # Send notifications if needed
            print(f"Checking disasters for user: {email}")

@celery_app.task
def update_disaster_hotspots():
    """Periodic task to update disaster hotspots statistics"""
    asyncio.run(async_update_hotspots())

async def async_update_hotspots():
    """Update hotspots data"""
    disasters = await nasa_client.fetch_nasa_events()
    # Process and store hotspot statistics
    print(f"Updated hotspots: {len(disasters)} disasters processed")

# Configure periodic tasks
celery_app.conf.beat_schedule = {
    "check-disasters-every-hour": {
        "task": "app.workers.tasks.check_disasters_for_all_users",
        "schedule": 3600.0,  # Every hour
    },
    "update-hotspots-daily": {
        "task": "app.workers.tasks.update_disaster_hotspots",
        "schedule": 86400.0,  # Every day
    },
}