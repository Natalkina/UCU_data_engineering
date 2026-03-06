from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import auth, disasters, calendar

app = FastAPI(
    title="Disaster Tracker API",
    description="Real-time disaster tracking and calendar integration",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(disasters.router)
app.include_router(calendar.router)

@app.get("/")
async def root():
    return {
        "message": "Disaster Tracker API",
        "docs": "/docs",
        "endpoints": {
            "auth": "/auth",
            "disasters": "/disasters",
            "calendar": "/calendar"
        }
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)