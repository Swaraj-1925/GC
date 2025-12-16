import asyncio
import json
import logging
from typing import Optional, Set, Dict
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Manages WebSocket connections for broadcast."""

    def __init__(self):
        # Connections by channel type and symbol
        self.tick_connections: Dict[str, Set[WebSocket]] = {}
        self.analytics_connections: Dict[str, Set[WebSocket]] = {}
        self.alert_connections: Set[WebSocket] = set()

    async def connect_ticks(self, websocket: WebSocket, symbol: str):
        """Connect a client to tick updates for a symbol."""
        await websocket.accept()
        if symbol not in self.tick_connections:
            self.tick_connections[symbol] = set()
        self.tick_connections[symbol].add(websocket)
        logger.info(f"Client connected to ticks/{symbol}")

    async def connect_analytics(self, websocket: WebSocket, symbol: str):
        """Connect a client to analytics updates for a symbol."""
        await websocket.accept()
        if symbol not in self.analytics_connections:
            self.analytics_connections[symbol] = set()
        self.analytics_connections[symbol].add(websocket)
        logger.info(f"Client connected to analytics/{symbol}")

    async def connect_alerts(self, websocket: WebSocket):
        """Connect a client to alert notifications."""
        await websocket.accept()
        self.alert_connections.add(websocket)
        logger.info("Client connected to alerts")

    def disconnect_ticks(self, websocket: WebSocket, symbol: str):
        """Disconnect a client from tick updates."""
        if symbol in self.tick_connections:
            self.tick_connections[symbol].discard(websocket)
            logger.info(f"Client disconnected from ticks/{symbol}")

    def disconnect_analytics(self, websocket: WebSocket, symbol: str):
        """Disconnect a client from analytics updates."""
        if symbol in self.analytics_connections:
            self.analytics_connections[symbol].discard(websocket)
            logger.info(f"Client disconnected from analytics/{symbol}")

    def disconnect_alerts(self, websocket: WebSocket):
        """Disconnect a client from alerts."""
        self.alert_connections.discard(websocket)
        logger.info("Client disconnected from alerts")

    async def broadcast_tick(self, symbol: str, data: dict):
        """Broadcast tick to all connected clients for a symbol."""
        if symbol not in self.tick_connections:
            return

        dead_connections = set()
        message = json.dumps(data)

        for websocket in self.tick_connections[symbol]:
            try:
                await websocket.send_text(message)
            except Exception:
                dead_connections.add(websocket)

        # Clean up dead connections
        self.tick_connections[symbol] -= dead_connections

    async def broadcast_analytics(self, symbol: str, data: dict):
        """Broadcast analytics to all connected clients for a symbol."""
        if symbol not in self.analytics_connections:
            return

        dead_connections = set()
        message = json.dumps(data)

        for websocket in self.analytics_connections[symbol]:
            try:
                await websocket.send_text(message)
            except Exception:
                dead_connections.add(websocket)

        self.analytics_connections[symbol] -= dead_connections

    async def broadcast_alert(self, data: dict):
        """Broadcast alert to all connected clients."""
        dead_connections = set()
        message = json.dumps(data)

        for websocket in self.alert_connections:
            try:
                await websocket.send_text(message)
            except Exception:
                dead_connections.add(websocket)

        self.alert_connections -= dead_connections


# Global connection manager
manager = ConnectionManager()
