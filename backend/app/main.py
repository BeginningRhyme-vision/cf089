from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import auth, metadata, jobs, youtube_jobs
from .database import engine, Base

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Unbound Future Admin API")

# CORS for frontend
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(metadata.router)
app.include_router(jobs.router)
app.include_router(youtube_jobs.router)

@app.get("/")
def root():
    return {"message": "Unbound Future Admin API is running"}
