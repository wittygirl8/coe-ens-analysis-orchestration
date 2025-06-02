from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import os
from app.api.api_router import api_router
from app.core.config import get_settings
from neo4j import AsyncGraphDatabase
app = FastAPI(
    title="minimal fastapi postgres template",
    version="6.1.0",
    description="https://github.com/20230028426_EYGS/coe-ens-analysis-orchestration.git",
    openapi_url="/openapi.json",
    docs_url="/",
)

app.include_router(api_router)
print("GRAPHDB__URI", os.environ.get("GRAPHDB__URI"))
# Sets all CORS enabled origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        str(origin).rstrip("/")
        for origin in get_settings().security.backend_cors_origins
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Guards against HTTP Host Header attacks
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=get_settings().security.allowed_hosts,
)

@app.on_event("startup")
async def startup_event():
    try:
        driver = AsyncGraphDatabase.driver(
            os.environ.get("GRAPHDB__URI"),
            auth=(os.environ.get("GRAPHDB__USER"), os.environ.get("GRAPHDB__PASSWORD")),
        )
        async with driver.session() as session:
            await session.run("RETURN 1")
        print("Neo4j connection established.")
    except Exception as e:
        print("Failed to connect to Neo4j:", str(e))