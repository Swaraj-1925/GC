import redis.asyncio as redis
from src.shared.config import REDIS_URL

def create_redis_client() -> redis.Redis:
    return redis.Redis.from_url(
        REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        health_check_interval=30
    )
