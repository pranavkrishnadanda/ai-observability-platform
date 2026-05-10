-- Enable required extensions
-- These must exist before table creation (which is handled by alembic migrations)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- GIN index for full-text search on logs.message will be created after table exists
-- See alembic migration for table creation
-- This file runs before alembic and just sets up extensions
