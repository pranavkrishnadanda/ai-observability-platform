from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql+asyncpg://aiobs:aiobs_secret@localhost:5432/aiobs_db"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"

    # Anthropic
    ANTHROPIC_API_KEY: str = ""

    # Auth
    SECRET_KEY: str = "change-me-in-production-use-32-char-minimum"
    API_KEY_PREFIX: str = "aiobs_"

    # Rate limiting
    RATE_LIMIT_PER_MINUTE: int = 1000
    ALERT_RATE_LIMIT_PER_HOUR: int = 10

    # Anomaly detection
    ANOMALY_CHECK_INTERVAL: int = 30  # seconds
    ANOMALY_VOLUME_SPIKE_MULTIPLIER: float = 2.5
    ANOMALY_VOLUME_DROP_MULTIPLIER: float = 0.2
    ANOMALY_ERROR_RATE_MULTIPLIER: float = 3.0

    # Log retention
    LOG_RETENTION_HOT_HOURS: int = 1
    LOG_RETENTION_COLD_DAYS: int = 90

    # Claude
    CLAUDE_MODEL: str = "claude-haiku-4-5-20251001"
    CLAUDE_TIMEOUT: int = 30
    CLAUDE_MAX_CONCURRENT: int = 10

    # Application
    APP_NAME: str = "AI Observability Platform"
    DEBUG: bool = False

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
