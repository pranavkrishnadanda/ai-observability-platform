"""
Webhook delivery with HMAC-SHA256 signing and retry logic.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import time
from typing import Optional

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)

RETRY_DELAYS = [1, 5, 30]  # seconds for attempts 2, 3, 4


def sign_payload(payload_bytes: bytes, secret: str) -> str:
    """HMAC-SHA256 signature for webhook payload."""
    return hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()


async def deliver_webhook(
    webhook_url: str,
    payload: dict,
    alert_id: str,
    secret: str = None,
) -> tuple[bool, Optional[str]]:
    """
    Attempt webhook delivery with retries.
    Returns (success: bool, error_message: Optional[str]).
    """
    payload_bytes = json.dumps(payload, default=str).encode("utf-8")
    signature = sign_payload(payload_bytes, secret or settings.SECRET_KEY)

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Signature": f"sha256={signature}",
        "X-Alert-ID": alert_id,
        "User-Agent": "AI-Observability-Platform/1.0",
    }

    last_error = None
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt, delay in enumerate([0] + RETRY_DELAYS, start=1):
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                response = await client.post(
                    webhook_url,
                    content=payload_bytes,
                    headers=headers,
                )
                response.raise_for_status()
                logger.info(
                    f"Webhook delivered to {webhook_url} on attempt {attempt}, alert={alert_id}"
                )
                return True, None
            except httpx.HTTPStatusError as e:
                last_error = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
                logger.warning(
                    f"Webhook attempt {attempt} failed ({webhook_url}): {last_error}"
                )
            except httpx.RequestError as e:
                last_error = f"Request error: {str(e)}"
                logger.warning(
                    f"Webhook attempt {attempt} failed ({webhook_url}): {last_error}"
                )

    logger.error(
        f"Webhook delivery failed after {len(RETRY_DELAYS) + 1} attempts, alert={alert_id}: {last_error}"
    )
    return False, last_error
