"""
Shared test fixtures for the AI Observability Platform test suite.

Design decisions:
- SQLite in-memory via aiosqlite for fast, isolated DB per test function
- All external services (Redis, Kafka, Anthropic) are fully mocked
- FastAPI dependencies are overridden to bypass bcrypt / real Redis auth
- Each test gets a fresh DB schema (create_all / drop_all)
"""
import asyncio
import uuid
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

# ── Patch kafka + redis module-level singletons BEFORE importing main app ──────
# The app module imports kafka_client and redis_client at import time; patching
# the globals prevents lifespan from trying to connect to real brokers.
import app.core.kafka_client as _kc
import app.core.redis_client as _rc

_mock_producer_global = MagicMock()
_mock_producer_global.send = MagicMock(return_value=MagicMock())
_mock_producer_global.flush = MagicMock()
_mock_producer_global.close = MagicMock()
_mock_producer_global.bootstrap_connected = MagicMock(return_value=True)
_kc._producer = _mock_producer_global

_mock_redis_global = AsyncMock()
_mock_redis_global.ping = AsyncMock(return_value=True)
_mock_redis_global.aclose = AsyncMock()
_rc._pool = _mock_redis_global

# Now it's safe to import the app
from main import app  # noqa: E402
from app.core.auth import TenantContext, get_current_tenant  # noqa: E402
from app.core.database import get_db  # noqa: E402
from app.core.redis_client import get_redis  # noqa: E402
from app.models.base import Base  # noqa: E402
from app.models.tenants import Tenant  # noqa: E402
from app.core.auth import generate_api_key  # noqa: E402

from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# ── SQLite test database path ─────────────────────────────────────────────────
# Use a file-based SQLite so aiosqlite works reliably across connections.
TEST_DB_URL = "sqlite+aiosqlite:///./test_db.db"


# ── Session-scoped event loop ─────────────────────────────────────────────────

@pytest.fixture(scope="session")
def event_loop():
    """Single event loop for the entire test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ── Per-test SQLite engine & session factory ──────────────────────────────────

@pytest_asyncio.fixture()
async def db_engine():
    """Create a fresh SQLite engine + schema for each test, then tear it down."""
    engine = create_async_engine(
        TEST_DB_URL,
        echo=False,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture()
async def db_session(db_engine) -> AsyncGenerator[AsyncSession, None]:
    """Provide an async DB session backed by the per-test SQLite engine."""
    factory = async_sessionmaker(
        db_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )
    async with factory() as session:
        yield session


# ── Mock Redis fixture ────────────────────────────────────────────────────────

@pytest_asyncio.fixture()
async def mock_redis():
    """
    Fully mocked async Redis instance.
    incr returns 1 by default (under rate limit).
    pipeline() returns a context-manager-compatible async mock.
    """
    redis = AsyncMock()

    # Common commands
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.setex = AsyncMock(return_value=True)
    redis.incr = AsyncMock(return_value=1)
    redis.expire = AsyncMock(return_value=True)
    redis.sadd = AsyncMock(return_value=1)
    redis.sismember = AsyncMock(return_value=False)
    redis.smembers = AsyncMock(return_value=set())
    redis.lrange = AsyncMock(return_value=[])
    redis.hgetall = AsyncMock(return_value={})
    redis.delete = AsyncMock(return_value=1)
    redis.keys = AsyncMock(return_value=[])
    redis.ping = AsyncMock(return_value=True)
    redis.aclose = AsyncMock()
    redis.zadd = AsyncMock(return_value=1)
    redis.zrangebyscore = AsyncMock(return_value=[])

    # Pipeline context manager
    pipe = AsyncMock()
    pipe.incr = AsyncMock()
    pipe.get = AsyncMock()
    pipe.set = AsyncMock()
    pipe.sadd = AsyncMock()
    pipe.expire = AsyncMock()
    pipe.execute = AsyncMock(return_value=[1, True])
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    redis.pipeline = MagicMock(return_value=pipe)

    return redis


# ── Mock Kafka producer fixture ───────────────────────────────────────────────

@pytest.fixture()
def mock_kafka_producer():
    """MagicMock Kafka producer with send/flush/close stubs."""
    producer = MagicMock()
    producer.send = MagicMock(return_value=MagicMock())
    producer.flush = MagicMock()
    producer.close = MagicMock()
    producer.bootstrap_connected = MagicMock(return_value=True)
    return producer


# ── Mock Anthropic fixture ────────────────────────────────────────────────────

@pytest.fixture()
def mock_anthropic():
    """Mock Anthropic client returning a valid Claude JSON analysis response."""
    import json

    analysis_json = json.dumps({
        "severity_assessment": "HIGH",
        "root_cause": "Sudden increase in request volume due to traffic spike",
        "recommended_actions": ["Scale up replicas", "Enable rate limiting"],
        "confidence": 0.85,
    })

    mock_content = MagicMock()
    mock_content.text = analysis_json

    mock_message = MagicMock()
    mock_message.content = [mock_content]

    mock_client = MagicMock()
    mock_client.messages = MagicMock()
    mock_client.messages.create = MagicMock(return_value=mock_message)

    return mock_client


# ── Tenant fixture (real DB row) ───────────────────────────────────────────────

@pytest_asyncio.fixture()
async def test_tenant(db_session) -> tuple[Tenant, str]:
    """
    Creates a real Tenant row in the test SQLite DB.
    Returns (tenant_orm_object, raw_api_key).
    """
    raw_key, hashed_key = generate_api_key()
    tenant = Tenant(
        id=uuid.uuid4(),
        name="test-tenant",
        api_key_hash=hashed_key,
        plan_tier="pro",
        rate_limit_per_minute=10000,
        webhook_url="https://hooks.example.com/test",
        alert_thresholds={},
        retention_days=90,
        is_active=True,
    )
    db_session.add(tenant)
    await db_session.commit()
    await db_session.refresh(tenant)
    return tenant, raw_key


# ── FastAPI async test client ─────────────────────────────────────────────────

@pytest_asyncio.fixture()
async def client(db_session, mock_redis, mock_kafka_producer, test_tenant):
    """
    httpx.AsyncClient backed by ASGI transport.
    Overrides:
      - get_db        → test SQLite session
      - get_redis     → mock_redis
      - get_current_tenant → bypass bcrypt, return TenantContext directly
    """
    tenant_obj, raw_key = test_tenant
    tenant_id = str(tenant_obj.id)

    # Override get_db to use test session
    async def override_get_db():
        yield db_session

    # Override get_redis to use mock
    async def override_get_redis():
        return mock_redis

    # Override get_current_tenant to skip bcrypt entirely
    async def override_get_current_tenant():
        return TenantContext(
            tenant_id=tenant_id,
            name="test-tenant",
            plan_tier="pro",
            rate_limit_per_minute=10000,
            webhook_url="https://hooks.example.com/test",
        )

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis
    app.dependency_overrides[get_current_tenant] = override_get_current_tenant

    # Patch the module-level kafka producer used by publish_async
    import app.core.kafka_client as kafka_mod
    original_producer = kafka_mod._producer
    kafka_mod._producer = mock_kafka_producer

    # Patch the module-level redis pool used by rate_limit_check
    import app.core.redis_client as redis_mod
    original_redis_pool = redis_mod._pool
    redis_mod._pool = mock_redis

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as ac:
        yield ac

    # Restore overrides
    app.dependency_overrides.clear()
    kafka_mod._producer = original_producer
    redis_mod._pool = original_redis_pool


# ── API headers fixture ───────────────────────────────────────────────────────

@pytest_asyncio.fixture()
async def api_headers(test_tenant):
    """Returns {"X-API-Key": raw_key} for authenticated requests."""
    _, raw_key = test_tenant
    return {"X-API-Key": raw_key}
