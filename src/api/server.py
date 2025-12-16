import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ..shared.config import get_settings
from ..shared.db.redis_client import RedisClient
from ..shared.db.timescale_client import TimescaleClient
from .endpoints import analytics, websocket_routes

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    logger.info("Starting API Gateway...")

    redis_client = RedisClient("api_gateway")
    await redis_client.connect()
    app.state.redis = redis_client
    # Initialize TimescaleDB connection for API (shared by all endpoints)
    timescale_client = TimescaleClient("api_gateway")
    await timescale_client.connect()
    app.state.timescale = timescale_client

    logger.info("API Gateway started - Redis and TimescaleDB connected")

    yield

    logger.info("Shutting down API Gateway...")

    if hasattr(app.state, 'redis') and app.state.redis:
        await app.state.redis.disconnect()

    if hasattr(app.state, 'timescale') and app.state.timescale:
        await app.state.timescale.disconnect()

    logger.info("API Gateway shutdown complete")


async def get_redis(request: Request) -> RedisClient:
    """FastAPI dependency to get Redis client from app state."""
    return request.app.state.redis


async def get_timescale(request: Request) -> TimescaleClient:
    """FastAPI dependency to get TimescaleDB client from app state."""
    return request.app.state.timescale


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title=settings.APP_NAME,
        description="Real-time market data analytics API",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(analytics.router, prefix="/api", tags=["Analytics"])
    app.include_router(websocket_routes.router, prefix="/ws", tags=["WebSocket"])

    # Health check endpoint
    @app.get("/health", tags=["Health"])
    async def health_check(request: Request) -> Dict[str, Any]:
        """Check API health and connectivity."""
        redis_status = "unknown"
        timescale_status = "unknown"

        try:
            if hasattr(request.app.state, 'redis') and request.app.state.redis:
                await request.app.state.redis._client.ping()
                redis_status = "connected"
            else:
                redis_status = "not initialized"
        except Exception as e:
            redis_status = f"error: {str(e)}"

        try:
            if hasattr(request.app.state, 'timescale') and request.app.state.timescale:
                # Quick query to verify connection
                async with request.app.state.timescale._pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                timescale_status = "connected"
            else:
                timescale_status = "not initialized"
        except Exception as e:
            timescale_status = f"error: {str(e)}"

        is_healthy = redis_status == "connected" and timescale_status == "connected"

        return {
            "status": "healthy" if is_healthy else "degraded",
            "redis": redis_status,
            "timescale": timescale_status,
            "version": "1.0.0"
        }

    @app.get("/", tags=["Root"])
    async def root() -> Dict[str, str]:
        return {
            "name": settings.APP_NAME,
            "docs": "/docs",
            "health": "/health"
        }

    @app.exception_handler(Exception)
    async def global_exception_handler(request, exc):
        logger.error(f"Unhandled exception: {exc}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )

    return app


app = create_app()


def run_server():
    import uvicorn
    settings = get_settings()

    uvicorn.run(
        "src.api.server:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
        log_level="info"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )
    run_server()
