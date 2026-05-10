import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.database import engine  # noqa: F401 — imported to satisfy lifespan reference
from app.core.kafka_client import get_producer
from app.core.redis_client import get_redis_pool
from app.api.v1 import (
    alerts,
    analysis,
    analytics,
    anomalies,
    ingest,
    logs,
    tenants,
    websocket,
)

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # ── Startup ──────────────────────────────────────────────────────────────
    logger.info("Starting up AI Observability Platform…")
    app.state.redis = await get_redis_pool()
    app.state.kafka_producer = get_producer()
    logger.info("Startup complete.")
    yield
    # ── Shutdown ─────────────────────────────────────────────────────────────
    logger.info("Shutting down…")
    await app.state.redis.aclose()
    app.state.kafka_producer.close()
    logger.info("Shutdown complete.")


app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    lifespan=lifespan,
)

# ── Middleware ────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):  # type: ignore[no-untyped-def]
    start = time.monotonic()
    response = await call_next(request)
    duration_ms = (time.monotonic() - start) * 1000
    logger.info(
        "%s %s %s %.1fms",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    response.headers["X-Response-Time"] = f"{duration_ms:.1f}ms"
    return response


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled exception: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# ── Routers ───────────────────────────────────────────────────────────────────

app.include_router(tenants.router, prefix="/api/v1", tags=["auth"])
app.include_router(ingest.router, prefix="/api/v1", tags=["ingestion"])
app.include_router(logs.router, prefix="/api/v1", tags=["logs"])
app.include_router(anomalies.router, prefix="/api/v1", tags=["anomalies"])
app.include_router(alerts.router, prefix="/api/v1", tags=["alerts"])
app.include_router(analytics.router, prefix="/api/v1", tags=["analytics"])
app.include_router(analysis.router, prefix="/api/v1", tags=["analysis"])
app.include_router(websocket.router, prefix="/ws", tags=["websocket"])


# ── Health ────────────────────────────────────────────────────────────────────


@app.get("/health", tags=["health"])
async def health_check(request: Request) -> JSONResponse:
    """Liveness + dependency health probe."""
    components: dict[str, str] = {}
    overall = "healthy"

    # PostgreSQL
    try:
        from sqlalchemy import text
        from app.core.database import AsyncSessionLocal

        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        components["postgres"] = "up"
    except Exception as exc:
        components["postgres"] = f"down: {exc}"
        overall = "degraded"

    # Redis
    try:
        redis = request.app.state.redis
        await redis.ping()
        components["redis"] = "up"
    except Exception as exc:
        components["redis"] = f"down: {exc}"
        overall = "degraded"

    # Kafka
    try:
        producer = request.app.state.kafka_producer
        connected = producer.bootstrap_connected()
        components["kafka"] = "up" if connected else "down: not connected"
        if not connected:
            overall = "degraded"
    except Exception as exc:
        components["kafka"] = f"down: {exc}"
        overall = "degraded"

    status_code = 200 if overall == "healthy" else 503
    return JSONResponse(
        status_code=status_code,
        content={
            "status": overall,
            "components": components,
            "timestamp": time.time(),
        },
    )
