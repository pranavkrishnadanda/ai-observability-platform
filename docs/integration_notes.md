# Integration Notes

## Frontend ↔ Backend Connection

- Frontend dev server runs on port 3000 (Vite proxy forwards /api and /ws to backend:8000)
- Production: frontend served by nginx on port 80/3000, backend on port 8000
- API auth: X-API-Key header injected by axios interceptor from localStorage
- WebSocket auth: token query param (?token=KEY) since browser WebSocket API doesn't support custom headers

## CORS
- Backend allows all origins in development
- For production, restrict to your frontend domain

## WebSocket Reconnect Strategy
- Exponential backoff: 1s → 2s → 4s → 8s → 16s → 30s (capped)
- On reconnect, message buffer is preserved (up to 500 messages)
- Connection displayed in LogStream status bar

## Local Development
1. Start infra only: `docker compose up -d postgres redis zookeeper kafka-1 kafka-2 kafka-3 kafka-init`
2. Run migrations: `cd backend && uv run alembic upgrade head`
3. Start backend: `cd backend && uv run uvicorn main:app --reload --port 8000`
4. Start workers (each in its own terminal):
   - `cd backend && uv run python -m app.consumers.log_consumer`
   - `cd backend && uv run python -m app.consumers.anomaly_consumer`
   - `cd backend && uv run python -m app.services.alert_engine`
5. Start frontend: `cd frontend && bun run dev`
6. Open http://localhost:3000
7. Register via UI (Register tab) or curl:
   ```bash
   curl -X POST http://localhost:8000/api/v1/auth/register \
     -H "Content-Type: application/json" -d '{"name":"my-org"}'
   ```
   Enter the returned `api_key` in the Sign In tab.

## Kafka Dual-Listener Setup (Local Dev)

Each Kafka broker exposes two listeners:
- `PLAINTEXT` on the internal Docker port — used for inter-broker traffic and Docker-internal consumers
- `PLAINTEXT_HOST` on the host-mapped port — used when FastAPI runs locally (outside Docker)

| Broker | Internal (Docker) | Host-accessible |
|--------|-------------------|-----------------|
| kafka-1 | `kafka-1:9092` | `localhost:9092` |
| kafka-2 | `kafka-2:9093` | `localhost:9093` |
| kafka-3 | `kafka-3:9094` | `localhost:9094` |

`backend/.env` uses `localhost:9092,localhost:9093,localhost:9094`.
`docker-compose.yml` overrides this with `kafka-1:9092,kafka-2:9093,kafka-3:9094` for containerized services.

## Auth Cache

Tenant API keys are cached in Redis as `auth:tenant_list` with a 30 s TTL. The cache stores a list of all active tenants' hashed keys so auth doesn't hit Postgres on every request.

- Cache is invalidated automatically on `POST /auth/register` and `POST /auth/rotate-key`.
- If you add a tenant directly to the database (bypassing the API), run `redis-cli DEL auth:tenant_list` to force a refresh.
- During local dev with DEBUG=true, bcrypt rounds are reduced to speed up auth.
