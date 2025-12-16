import uvicorn
import asyncio
import signal
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import List

import uvicorn

from src.shared.config import get_settings
from src.services.market_gateway import MarketGateway
from src.services.quant_engine import QuantEngine
from src.services.central_logger import CentralLogger
from src.services.archivist import Archivist

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ServiceOrchestrator:
    """
    Orchestrates all backend services.

    Runs each service in its own thread/task with proper
    lifecycle management and graceful shutdown.
    """

    def __init__(self):
        self.settings = get_settings()
        self.running = True

        # Service instances
        self.market_gateway: MarketGateway = None
        self.quant_engine: QuantEngine = None
        self.central_logger: CentralLogger = None
        self.archivist: Archivist = None

        self.tasks: List[asyncio.Task] = []

    async def start_all_services(self):
        """Start all services as async tasks."""
        logger.info("=" * 60)
        logger.info("GemScap Quant Analytics - Starting Services")
        logger.info("=" * 60)
        logger.info(f"Symbols: {self.settings.SYMBOLS}")
        # logger.info(f"Redis: {self.settings.REDIS_HOST}:{self.settings.REDIS_PORT}")
        logger.info(f"API: http://{self.settings.API_HOST}:{self.settings.API_PORT}")
        logger.info("=" * 60)

        # Initialize services
        self.market_gateway = MarketGateway(self.settings.SYMBOLS)
        self.quant_engine = QuantEngine(
            self.settings.SYMBOLS,
            window_size=self.settings.ROLLING_WINDOW_TICKS,
            alert_z_threshold=self.settings.Z_SCORE_ALERT_THRESHOLD
        )
        self.central_logger = CentralLogger(self.settings.LOG_DIR)
        self.archivist = Archivist(
            self.settings.SYMBOLS,
            archive_interval=self.settings.ARCHIVE_INTERVAL_SECONDS,
            batch_size=self.settings.ARCHIVE_BATCH_SIZE
        )
        self.tasks = [
            asyncio.create_task(self._run_market_gateway(), name="market_gateway"),
            asyncio.create_task(self._run_quant_engine(), name="quant_engine"),
            asyncio.create_task(self._run_central_logger(), name="central_logger"),
            asyncio.create_task(self._run_archivist(), name="archivist"),
            ]
        logger.info("All services started")

        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("Services cancelled")

    async def _run_market_gateway(self):
        """Run market gateway with error handling."""
        try:
            logger.info("[MarketGateway] Starting...")
            await self.market_gateway.start()
        except Exception as e:
            logger.error(f"[MarketGateway] Error: {e}")
        finally:
            logger.info("[MarketGateway] Stopped")

    async def _run_quant_engine(self):
        """Run quant engine with error handling."""
        # Small delay to let market gateway start first
        await asyncio.sleep(2)
        try:
            logger.info("[QuantEngine] Starting...")
            await self.quant_engine.start()
        except Exception as e:
            logger.error(f"[QuantEngine] Error: {e}")
        finally:
            logger.info("[QuantEngine] Stopped")

    async def _run_central_logger(self):
        """Run central logger with error handling."""
        try:
            logger.info("[CentralLogger] Starting...")
            await self.central_logger.start()
        except Exception as e:
            logger.error(f"[CentralLogger] Error: {e}")
        finally:
            logger.info("[CentralLogger] Stopped")

    async def _run_archivist(self):
        """Run archivist with error handling."""
        # Delay to let other services start
        await asyncio.sleep(5)
        try:
            logger.info("[Archivist] Starting...")
            await self.archivist.start()
        except Exception as e:
            logger.error(f"[Archivist] Error: {e}")
        finally:
            logger.info("[Archivist] Stopped")

    async def stop_all_services(self):
        """Stop all services gracefully."""
        logger.info("Stopping all services...")
        self.running = False

        # Stop services
        if self.market_gateway:
            await self.market_gateway.stop()
        if self.quant_engine:
            await self.quant_engine.stop()
        if self.central_logger:
            await self.central_logger.stop()
        if self.archivist:
            await self.archivist.stop()
        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        logger.info("All services stopped")

def run_api_server():
    """Run the FastAPI server in a separate thread."""
    settings = get_settings()

    uvicorn.run(
        "src.api.server:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level="info",
        access_log=False  # Reduce log noise
    )


async def main():
    """Main entry point."""
    orchestrator = ServiceOrchestrator()
    settings = get_settings()

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()

    def signal_handler():
        logger.info("Received shutdown signal")
        asyncio.create_task(orchestrator.stop_all_services())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    executor = ThreadPoolExecutor(max_workers=1)
    loop.run_in_executor(executor, run_api_server)

    logger.info(f"API server starting at http://{settings.API_HOST}:{settings.API_PORT}")
    logger.info(f"API docs at http://{settings.API_HOST}:{settings.API_PORT}/docs")
    try:
        await orchestrator.start_all_services()
    except KeyboardInterrupt:
        pass
    finally:
        await orchestrator.stop_all_services()
        executor.shutdown(wait=False)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown complete")
