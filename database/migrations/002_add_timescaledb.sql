-- database/migrations/002_add_timescaledb.sql
-- Migration 002: Convert time-series tables to TimescaleDB hypertables.
-- Run AFTER 001_initial_schema.sql AND after installing TimescaleDB extension.
--
-- Author : Julius Marselinus (BRONTO) - NIM 00000111989

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

SELECT create_hypertable(
    'satellite_scenes', 'acquisition_datetime',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

SELECT create_hypertable(
    'api_access_logs', 'request_timestamp',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT create_hypertable(
    'alert_events', 'triggered_at',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

INSERT INTO schema_migrations (version, description)
VALUES ('002', 'TimescaleDB hypertables: satellite_scenes, api_access_logs, alert_events')
ON CONFLICT (version) DO NOTHING;
