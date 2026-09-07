-- database/migrations/001_initial_schema.sql
-- Migration 001: Initial schema creation (delegates to schema.sql).
-- Use this file with Alembic or manual migration tracking.
--
-- Author : Julius Marselinus (BRONTO) - NIM 00000111989

-- Record migration
INSERT INTO schema_migrations (version, description)
VALUES ('001', 'Initial schema: 11 master tables, PostGIS, enums, triggers, seed stages')
ON CONFLICT (version) DO NOTHING;

-- Note: Full DDL is in database/schema.sql
-- Run: psql sentinel1_flood < database/schema.sql
