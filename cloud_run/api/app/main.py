import app.api.main as api
from fastapi.middleware.cors import CORSMiddleware

# from scrapers.main import scrapers

from typing import Optional

from fastapi import FastAPI

app = FastAPI()

# Allow all origins
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api.router)


@app.get("/")
async def root():
    return {"message": "Hello Bigger Applications!"}
