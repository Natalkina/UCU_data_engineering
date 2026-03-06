# Disaster Tracker API

Real-time disaster tracking system with Google Calendar integration and automated notifications.

## Features

- **Real-time Disaster Events**: Integration with NASA EONET and PDC APIs
- **Google Calendar Integration**: Track user events and check for disaster proximity
- **Automated Notifications**: Alert users when planned events are near disaster zones
- **Hotspot Analysis**: Track and identify disaster-prone areas worldwide
- **User Authentication**: JWT-based auth with user preferences
- **Background Tasks**: Celery workers for periodic disaster checking

## Architecture

- **FastAPI**: REST API framework
- **PostgreSQL**: User and preference storage
- **Redis**: Celery task queue
- **Celery**: Background task processing
- **Google Calendar API**: Calendar integration
- **NASA EONET API**: Disaster event data
- **PDC API**: Additional disaster alerts

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Run with Docker Compose
```bash
docker-compose up --build
```

### 4. Access API
- API: http://localhost:8000
- Docs: http://localhost:8000/docs

## API Endpoints

### Authentication
- `POST /auth/register` - Register new user
- `POST /auth/login` - Login and get token
- `GET /auth/me` - Get current user info

### Disasters
- `GET /disasters/events` - Get all disaster events
- `GET /disasters/events/by-date` - Filter by date range
- `GET /disasters/events/by-location` - Find disasters near location
- `GET /disasters/hotspots` - Get disaster hotspots

### Calendar
- `POST /calendar/connect-google` - Connect Google Calendar
- `GET /calendar/events` - Get user's calendar events
- `GET /calendar/check-disasters` - Check events against disasters

## Usage Example

### 1. Register User
```bash
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"pass123","name":"John"}'
```

### 2. Login
```bash
curl -X POST http://localhost:8000/auth/login \
  -d "username=user@example.com&password=pass123"
```

### 3. Get Disasters
```bash
curl http://localhost:8000/disasters/events
```

### 4. Check Disasters by Location
```bash
curl "http://localhost:8000/disasters/events/by-location?lat=40.7128&lon=-74.0060&radius_km=100"
```

### 5. Get Hotspots
```bash
curl http://localhost:8000/disasters/hotspots?limit=10
```

## Google Calendar Integration

1. Create Google Cloud Project
2. Enable Google Calendar API
3. Create OAuth 2.0 credentials
4. Add credentials to `.env`
5. Connect calendar via `/calendar/connect-google`

## Background Tasks

Celery workers run periodic tasks:
- **Hourly**: Check disasters for all users
- **Daily**: Update hotspot statistics

Start workers:
```bash
celery -A app.workers.celery_app worker --loglevel=info
celery -A app.workers.celery_app beat --loglevel=info
```

## Disaster Matching Logic

1. Fetch user's calendar events
2. Extract event locations
3. Geocode locations to coordinates
4. Compare with active disasters
5. Calculate distance between event and disaster
6. Generate warnings if within threshold (default 100km)
7. Send notifications via API

## Warning Levels

- **HIGH**: < 50km from disaster
- **MEDIUM**: 50-100km from disaster
- **LOW**: > 100km (configurable)

## Development

Run locally:
```bash
uvicorn app.main:app --reload
```

Run tests:
```bash
pytest tests/
```

## Configuration

Edit `.env` file:
- `DISASTER_RADIUS_KM`: Distance threshold for warnings
- `GOOGLE_CLIENT_ID`: Google OAuth client ID
- `GOOGLE_CLIENT_SECRET`: Google OAuth secret
- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string

## Data Sources

- **NASA EONET**: https://eonet.gsfc.nasa.gov/api/v3/events
- **PDC Disaster Alert**: https://disasteralert.pdc.org/disasteralert/

## License

MIT