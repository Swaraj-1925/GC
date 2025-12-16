from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.shared.db.redis import create_redis_client
from src.shared.db.postgres import create_pg_client

service_db_session = None
service_redis = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global service_db_session, service_redis
    engine, service_db_session = create_pg_client()
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    service_redis = create_redis_client()
    yield
    await service_redis.close()
    await engine.dispose()

app = FastAPI(lifespan=lifespan, title="GemsCap Global Analyst Pvt Ltd")
@app.get("/")
async def root():
    return "App Is Running"
