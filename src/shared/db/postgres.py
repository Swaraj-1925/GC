from sqlmodel import SQLModel
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from fastapi import Depends

from src.shared.config import POSTGRES_URL

def create_pg_client():
    engine = create_async_engine(
        POSTGRES_URL,
        echo=False,
        pool_size=5,
        max_overflow=10
    )

    AsyncSessionLocal = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    return engine, AsyncSessionLocal
