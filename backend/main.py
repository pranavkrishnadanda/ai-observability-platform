import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.database import engine  # noqa: F401 — imported to satisfy lifespan reference
from app.core.kafka_client import get_producer
from app.core.redis_client import get_redis_pool
import app.consumers.log_consumer as _log_consumer_module
import app.consumers.anomaly_consumer as _anomaly_consumer_module
import app.services.alert_engine as _alert_engine_module
from app.consumers.log_consumer import run_consumer
from app.consumers.anomaly_consumer import run_anomaly_consumer
from app.services.alert_engine import run_alert_engine
from app.services.anomaly_detector import run_anomaly_detection_loop
from app.services.baseline_calculator import run_baseline_calculator_loop

# Each consumer runs in its own thread (they use blocking KafkaConsumer.poll)
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="kafka-consumer")

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


# Sync wrappers so async consumers (which contain blocking poll() calls) run in
# their own threads with a fresh event loop — avoids blocking the FastAPI loop.
def _run_anomaly_consumer() -> None:
    asyncio.run(run_anomaly_consumer())


def _run_alert_engine() -> None:
    asyncio.run(run_alert_engine())


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # ── Startup ──────────────────────────────────────────────────────────────
    logger.info("Starting up AI Observability Platform…")
    app.state.redis = await get_redis_pool()
    app.state.kafka_producer = get_producer()
    logger.info("Startup complete.")

    anomaly_task = asyncio.create_task(
        run_anomaly_detection_loop(),
        name="anomaly-detector"
    )
    baseline_task = asyncio.create_task(
        run_baseline_calculator_loop(),
        name="baseline-calculator"
    )
    logger.info("Background schedulers started")

    # run_consumer() is sync (calls asyncio.run internally) — straight to executor.
    # _run_anomaly_consumer / _run_alert_engine are sync wrappers around async fns
    # that contain blocking KafkaConsumer.poll() — must not run on the FastAPI loop.
    loop = asyncio.get_running_loop()
    log_consumer_future = loop.run_in_executor(_executor, run_consumer)
    anomaly_consumer_future = loop.run_in_executor(_executor, _run_anomaly_consumer)
    alert_engine_future = loop.run_in_executor(_executor, _run_alert_engine)
    logger.info("Kafka consumers started")

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────
    logger.info("Shutting down…")

    # Signal all consumer loops to exit
    _log_consumer_module._shutdown = True
    _anomaly_consumer_module._shutdown = True
    _alert_engine_module._shutdown = True

    anomaly_task.cancel()
    baseline_task.cancel()
    await asyncio.gather(anomaly_task, baseline_task, return_exceptions=True)
    logger.info("Background schedulers stopped")

    # Wait up to 10 s for each consumer thread to finish cleanly
    for fut in (log_consumer_future, anomaly_consumer_future, alert_engine_future):
        try:
            await asyncio.wait_for(asyncio.wrap_future(fut), timeout=10.0)
        except (asyncio.TimeoutError, Exception):
            pass
    _executor.shutdown(wait=False)
    logger.info("Kafka consumers stopped")

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

    # Kafka — use partitions_for() which works once cluster metadata is populated
    # (bootstrap_connected() returns False after the bootstrap node disconnects,
    #  which is normal behavior once the producer has connected to the real brokers)
    try:
        producer = request.app.state.kafka_producer
        partitions = producer.partitions_for("logs.raw")
        if partitions:
            components["kafka"] = "up"
        else:
            components["kafka"] = "down: no partitions"
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
