import app.api.main as api
# from scrapers.main import scrapers

from typing import Optional

from fastapi import FastAPI

app = FastAPI()

app.include_router(api.router)


@app.get("/")
async def root():
    return {"message": "Hello Bigger Applications!"}
