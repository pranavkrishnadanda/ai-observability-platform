import json
from typing import Optional, List, Any
import redis.asyncio as aioredis
from app.core.config import settings

_pool: Optional[aioredis.Redis] = None


async def get_redis_pool() -> aioredis.Redis:
    global _pool
    if _pool is None:
        _pool = aioredis.from_url(
            settings.REDIS_URL,
            max_connections=100,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
            decode_responses=True,
        )
    return _pool


async def get_redis() -> aioredis.Redis:
    return await get_redis_pool()


async def rate_limit_check(key: str, limit: int, window: int) -> bool:
    """Fixed-window rate limit. Returns True if allowed, False if over limit."""
    redis = await get_redis_pool()
    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, window)
    return count <= limit


async def cache_get(key: str) -> Optional[dict]:
    redis = await get_redis_pool()
    value = await redis.get(key)
    if value is None:
        return None
    return json.loads(value)


async def cache_set(key: str, value: Any, ttl: int) -> None:
    redis = await get_redis_pool()
    await redis.setex(key, ttl, json.dumps(value, default=str))


async def cache_invalidate(pattern: str) -> None:
    redis = await get_redis_pool()
    keys = await redis.keys(pattern)
    if keys:
        await redis.delete(*keys)


async def timeseries_add(key: str, value: float, timestamp: float, ttl: int = 86400) -> None:
    """Store a timestamped value using a sorted set (score=timestamp, member=json)."""
    redis = await get_redis_pool()
    member = json.dumps({"ts": timestamp, "v": value})
    pipe = redis.pipeline()
    pipe.zadd(key, {member: timestamp})
    pipe.expire(key, ttl)
    await pipe.execute()


async def timeseries_range(key: str, start: float, end: float) -> List[dict]:
    """Get values between timestamps."""
    redis = await get_redis_pool()
    members = await redis.zrangebyscore(key, start, end)
    return [json.loads(m) for m in members]
