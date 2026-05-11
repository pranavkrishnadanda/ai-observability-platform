FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl liblz4-dev \
    && rm -rf /var/lib/apt/lists/*

# Install packages into system Python — lives in /usr/local/lib, NOT /app,
# so the bind-mount on /app at runtime will NOT wipe the installed packages.
RUN pip install --no-cache-dir \
      "lz4" \
      "fastapi==0.115.5" \
      "uvicorn[standard]==0.32.1" \
      "sqlalchemy[asyncio]==2.0.36" \
      "asyncpg==0.29.0" \
      "alembic==1.13.0" \
      "kafka-python-ng==2.2.3" \
      "redis[hiredis]==5.0.8" \
      "anthropic==0.40.0" \
      "pydantic-settings==2.5.2" \
      "python-jose[cryptography]==3.3.0" \
      "passlib[bcrypt]==1.7.4" \
      "bcrypt==3.2.2" \
      "python-multipart==0.0.17" \
      "httpx==0.27.0"

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
