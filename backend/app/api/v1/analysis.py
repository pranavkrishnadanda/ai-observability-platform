import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta

import anthropic
import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import TenantContext, get_current_tenant
from app.core.config import settings
from app.core.database import get_db
from app.core.redis_client import get_redis
from app.models.logs import Log

router = APIRouter()
logger = logging.getLogger(__name__)

ANALYSIS_SYSTEM_PROMPT = """You are an expert SRE analyzing a production anomaly. Analyze the provided logs and metrics, then return ONLY a JSON object with these exact fields:
{
  "root_cause": "One sentence describing likely cause",
  "confidence": 0.0,
  "affected_components": ["list", "of", "components"],
  "recommended_actions": ["action1", "action2", "action3"],
  "severity_assessment": "LOW|MEDIUM|HIGH|CRITICAL",
  "similar_incidents": "Description of similar patterns if seen in the log history",
  "estimated_resolution_time": "X minutes/hours"
}
Respond with ONLY the JSON. No markdown. No explanation."""


class RootCauseRequest(BaseModel):
    service: str
    from_time: datetime
    to_time: datetime

    @field_validator("to_time")
    @classmethod
    def validate_time_window(cls, to_time: datetime, info) -> datetime:
        from_time = info.data.get("from_time")
        if from_time and to_time < from_time:
            raise ValueError("to_time must be >= from_time")
        if from_time and (to_time - from_time) > timedelta(hours=24):
            raise ValueError("Time window must be <= 24 hours")
        return to_time


class CompareRequest(BaseModel):
    service: str
    period1_start: datetime
    period2_start: datetime
    hours: int = 1

    @field_validator("hours")
    @classmethod
    def validate_hours(cls, v: int) -> int:
        if not 1 <= v <= 24:
            raise ValueError("hours must be between 1 and 24")
        return v


async def call_claude_analysis(
    service: str,
    log_samples: list[dict],
    context: str,
    client: anthropic.AsyncAnthropic,
) -> dict:
    log_text = "\n".join(
        f"[{l.get('severity', '?')}] {str(l.get('message', ''))[:300]}"
        for l in log_samples[:50]
    )

    user_message = (
        f"Service: {service}\n"
        f"Context: {context}\n\n"
        f"Recent log samples:\n{log_text}\n\n"
        "Analyze and provide root cause assessment."
    )

    try:
        response = await asyncio.wait_for(
            client.messages.create(
                model=settings.CLAUDE_MODEL,
                max_tokens=1024,
                system=ANALYSIS_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}],
            ),
            timeout=settings.CLAUDE_TIMEOUT,
        )
        text = response.content[0].text.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(
                lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
            )
        return json.loads(text.strip())
    except asyncio.TimeoutError:
        raise HTTPException(status_code=503, detail="AI analysis timed out")
    except json.JSONDecodeError as e:
        logger.warning(f"Claude returned invalid JSON: {e}")
        raise HTTPException(status_code=503, detail="AI analysis returned invalid response")
    except anthropic.APIError as e:
        logger.error(f"Anthropic API error: {e}")
        raise HTTPException(status_code=503, detail=f"AI analysis failed: {str(e)}")
    except Exception as e:
        logger.error(f"Claude analysis failed: {e}")
        raise HTTPException(status_code=503, detail=f"AI analysis failed: {str(e)}")


@router.post("/analysis/root-cause")
async def root_cause_analysis(
    request: RootCauseRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    tid = uuid.UUID(tenant.tenant_id)

    # Fetch up to 50 logs in the requested window, most recent first
    result = await db.execute(
        select(Log)
        .where(
            and_(
                Log.tenant_id == tid,
                Log.service_name == request.service,
                Log.created_at >= request.from_time,
                Log.created_at <= request.to_time,
            )
        )
        .order_by(Log.created_at.desc())
        .limit(50)
    )
    logs = result.scalars().all()

    if not logs:
        raise HTTPException(
            status_code=404,
            detail="No logs found in the specified time window",
        )

    # Track per-tenant daily analysis credit usage (only charged when we actually call Claude)
    epoch_day = int(time.time()) // 86400
    credit_key = f"tenant:{tenant.tenant_id}:analysis_credits:{epoch_day}"
    credits_used = await redis.incr(credit_key)
    await redis.expire(credit_key, 172800)  # 2-day TTL

    log_samples = [
        {
            "severity": l.severity,
            "message": l.message,
            "created_at": str(l.created_at),
        }
        for l in logs
    ]

    # Collect evidence log IDs (top 5 error/critical logs)
    evidence_ids = [
        str(l.id)
        for l in logs
        if l.severity in ("ERROR", "CRITICAL")
    ][:5]

    context = (
        f"Time window: {request.from_time.isoformat()} to {request.to_time.isoformat()}, "
        f"log count: {len(logs)}"
    )

    claude_client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
    analysis = await call_claude_analysis(
        request.service, log_samples, context, claude_client
    )

    return {
        "service": request.service,
        "time_window": {
            "from": request.from_time.isoformat(),
            "to": request.to_time.isoformat(),
        },
        "log_count": len(logs),
        "evidence_log_ids": evidence_ids,
        "analysis": analysis,
        "credits_used_today": credits_used,
    }


@router.post("/analysis/compare")
async def compare_periods(
    request: CompareRequest,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    from datetime import timedelta as td

    tid = uuid.UUID(tenant.tenant_id)

    async def get_period_stats(start: datetime) -> dict:
        end = start + td(hours=request.hours)
        total_r = await db.execute(
            select(func.count(Log.id)).where(
                and_(
                    Log.tenant_id == tid,
                    Log.service_name == request.service,
                    Log.created_at >= start,
                    Log.created_at < end,
                )
            )
        )
        total = total_r.scalar() or 0

        errors_r = await db.execute(
            select(func.count(Log.id)).where(
                and_(
                    Log.tenant_id == tid,
                    Log.service_name == request.service,
                    Log.created_at >= start,
                    Log.created_at < end,
                    Log.severity.in_(["ERROR", "CRITICAL"]),
                )
            )
        )
        errors = errors_r.scalar() or 0

        return {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "volume": total,
            "errors": errors,
            "error_rate": round(errors / total, 4) if total else 0.0,
        }

    p1 = await get_period_stats(request.period1_start)
    p2 = await get_period_stats(request.period2_start)

    delta_volume = p2["volume"] - p1["volume"]
    volume_pct = (
        round(delta_volume / p1["volume"] * 100, 2) if p1["volume"] else None
    )
    delta_error_rate = p2["error_rate"] - p1["error_rate"]
    error_rate_pct = (
        round(delta_error_rate / p1["error_rate"] * 100, 2)
        if p1["error_rate"]
        else None
    )

    assessment = (
        "degraded"
        if delta_error_rate > 0.01
        else "improved"
        if delta_error_rate < -0.01
        else "stable"
    )

    # Fetch top new error messages appearing in period 2 but not period 1
    p1_start = request.period1_start
    p1_end = p1_start + td(hours=request.hours)
    p2_start = request.period2_start
    p2_end = p2_start + td(hours=request.hours)

    p1_errors_r = await db.execute(
        select(Log.message)
        .where(
            and_(
                Log.tenant_id == tid,
                Log.service_name == request.service,
                Log.created_at >= p1_start,
                Log.created_at < p1_end,
                Log.severity.in_(["ERROR", "CRITICAL"]),
            )
        )
        .limit(200)
    )
    p1_error_msgs = {row[0][:100] for row in p1_errors_r}

    p2_errors_r = await db.execute(
        select(Log.message)
        .where(
            and_(
                Log.tenant_id == tid,
                Log.service_name == request.service,
                Log.created_at >= p2_start,
                Log.created_at < p2_end,
                Log.severity.in_(["ERROR", "CRITICAL"]),
            )
        )
        .limit(200)
    )
    p2_error_msgs = [row[0][:100] for row in p2_errors_r]
    top_new_errors = list(
        dict.fromkeys(m for m in p2_error_msgs if m not in p1_error_msgs)
    )[:5]

    # Call Claude for comparison commentary if there are significant changes
    commentary = None
    if abs(delta_error_rate) > 0.005 or (volume_pct and abs(volume_pct) > 10):
        try:
            claude_client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
            compare_context = (
                f"Period 1 ({p1['start']} → {p1['end']}): "
                f"volume={p1['volume']}, errors={p1['errors']}, error_rate={p1['error_rate']}\n"
                f"Period 2 ({p2['start']} → {p2['end']}): "
                f"volume={p2['volume']}, errors={p2['errors']}, error_rate={p2['error_rate']}\n"
                f"New error patterns: {top_new_errors[:3]}"
            )
            compare_system = (
                "You are an SRE. Compare the two monitoring periods and provide a "
                "2-3 sentence commentary on key differences. Be specific and actionable. "
                "Return only the commentary text, no JSON."
            )
            response = await asyncio.wait_for(
                claude_client.messages.create(
                    model=settings.CLAUDE_MODEL,
                    max_tokens=256,
                    system=compare_system,
                    messages=[
                        {
                            "role": "user",
                            "content": f"Service: {request.service}\n{compare_context}",
                        }
                    ],
                ),
                timeout=settings.CLAUDE_TIMEOUT,
            )
            commentary = response.content[0].text.strip()
        except Exception as e:
            logger.warning(f"Claude comparison commentary failed: {e}")

    return {
        "service": request.service,
        "hours": request.hours,
        "period1": p1,
        "period2": p2,
        "delta": {
            "volume_pct": volume_pct,
            "error_rate_delta": round(delta_error_rate, 4),
            "error_rate_pct": error_rate_pct,
            "assessment": assessment,
        },
        "top_new_errors": top_new_errors,
        "claude_commentary": commentary,
    }
