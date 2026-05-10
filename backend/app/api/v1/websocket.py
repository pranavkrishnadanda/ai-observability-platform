import asyncio
import json
import logging
import time

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from sqlalchemy import select

from app.core.auth import verify_api_key
from app.core.database import AsyncSessionLocal
from app.core.redis_client import get_redis_pool
from app.models.tenants import Tenant

router = APIRouter()
logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 30  # seconds


async def authenticate_ws(token: str) -> str | None:
    """Authenticate a WebSocket connection via API key. Returns tenant_id or None."""
    if not token:
        return None
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Tenant).where(Tenant.is_active == True)  # noqa: E712
            )
            tenants = result.scalars().all()
            for t in tenants:
                if verify_api_key(token, t.api_key_hash):
                    return str(t.id)
    except Exception as e:
        logger.error(f"WebSocket authentication error: {e}")
    return None


async def _stream_channel(
    websocket: WebSocket,
    channel: str,
    message_type: str,
    tenant_id: str,
) -> None:
    """Generic pub/sub streaming loop with heartbeat support."""
    redis = await get_redis_pool()
    pubsub = redis.pubsub()
    await pubsub.subscribe(channel)
    last_heartbeat = time.monotonic()

    try:
        while True:
            # Non-blocking poll with a short timeout so heartbeat fires on time
            message = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=1.0
            )

            now = time.monotonic()
            if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                try:
                    await websocket.send_json({"type": "ping", "ts": time.time()})
                    last_heartbeat = now
                except WebSocketDisconnect:
                    break
                except Exception:
                    break

            if message is not None and message.get("type") == "message":
                try:
                    data = json.loads(message["data"])
                    await websocket.send_json({"type": message_type, "data": data})
                except WebSocketDisconnect:
                    break
                except Exception as e:
                    logger.warning(f"Failed to send WS {message_type} message: {e}")
                    break

            # Check for client-sent messages (e.g. pong) without blocking
            try:
                # receive_text with a zero-timeout to drain any pending frames
                client_msg = await asyncio.wait_for(
                    websocket.receive_text(), timeout=0.01
                )
                # We accept pong silently; ignore other messages
                _ = client_msg
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    finally:
        try:
            await pubsub.unsubscribe(channel)
        except Exception:
            pass
        try:
            await pubsub.aclose()
        except Exception:
            pass
        logger.info(
            f"WS {message_type} disconnected — channel={channel}, tenant={tenant_id}"
        )


@router.websocket("/logs/{service_name}")
async def websocket_logs(
    websocket: WebSocket,
    service_name: str,
    token: str = Query(...),
):
    tenant_id = await authenticate_ws(token)
    if not tenant_id:
        await websocket.close(code=4401, reason="Unauthorized")
        return

    await websocket.accept()
    logger.info(f"WS /logs/{service_name} connected — tenant={tenant_id}")

    channel = f"tenant:{tenant_id}:service:{service_name}:stream"
    await _stream_channel(websocket, channel, "log", tenant_id)


@router.websocket("/anomalies")
async def websocket_anomalies(
    websocket: WebSocket,
    token: str = Query(...),
):
    tenant_id = await authenticate_ws(token)
    if not tenant_id:
        await websocket.close(code=4401, reason="Unauthorized")
        return

    await websocket.accept()
    logger.info(f"WS /anomalies connected — tenant={tenant_id}")

    channel = f"tenant:{tenant_id}:anomalies:stream"
    await _stream_channel(websocket, channel, "anomaly", tenant_id)
