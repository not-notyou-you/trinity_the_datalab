# etl/config.py
"""
Centralized configuration management for the Sentinel-1 ETL pipeline.
Loads from environment variables with sensible defaults.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Usage:
    from etl.config import cfg
    print(cfg.output_dir)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # fine — use raw env vars


@dataclass
class DatabaseConfig:
    host:         str = field(default_factory=lambda: os.getenv("DB_HOST",     "localhost"))
    port:         int = field(default_factory=lambda: int(os.getenv("DB_PORT", "5432")))
    name:         str = field(default_factory=lambda: os.getenv("DB_NAME",     "sentinel1_flood"))
    user:         str = field(default_factory=lambda: os.getenv("DB_USER",     "postgres"))
    password:     str = field(default_factory=lambda: os.getenv("DB_PASSWORD", ""))
    pool_size:    int = field(default_factory=lambda: int(os.getenv("DB_POOL_SIZE",    "5")))
    max_overflow: int = field(default_factory=lambda: int(os.getenv("DB_MAX_OVERFLOW", "10")))
    echo:         bool = field(default_factory=lambda: os.getenv("DB_ECHO", "false").lower() == "true")

    @property
    def url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass
class APIConfig:
    host:  str = field(default_factory=lambda: os.getenv("API_HOST", "0.0.0.0"))
    port:  int = field(default_factory=lambda: int(os.getenv("API_PORT", "8000")))
    debug: bool = field(default_factory=lambda: os.getenv("API_DEBUG", "false").lower() == "true")


@dataclass
class PipelineConfig:
    output_dir:      str = field(default_factory=lambda: os.getenv("OUTPUT_DIR",     "processed"))
    logs_dir:        str = field(default_factory=lambda: os.getenv("LOGS_DIR",       "logs_pipeline"))
    checkpoint_dir:  str = field(default_factory=lambda: os.getenv("CHECKPOINT_DIR", "checkpoints_pipeline"))
    analytics_dir:   str = field(default_factory=lambda: os.getenv("ANALYTICS_DIR",  "analytics"))
    recovered_dir:   str = "recovered_temp"

    # Jabodetabek bounding box (WGS84)
    jabodetabek_bbox: tuple[float, float, float, float] = (106.4, -6.7, 107.2, -5.9)
    # (min_lon, min_lat, max_lon, max_lat)

    jabodetabek_wkt: str = (
        "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))"
    )

    # Lee filter defaults
    lee_window_size: int   = 7
    lee_looks:       int   = 1

    # COG export defaults
    cog_compression: str   = "LZW"
    cog_blocksize:   int   = 512
    cog_overviews:   list  = field(default_factory=lambda: [2, 4, 8, 16])

    # Quality thresholds
    min_quality_score:      float = 60.0
    max_nodata_percent:     float = 30.0
    cloud_threshold_percent: float = 20.0

    def ensure_dirs(self) -> None:
        """Create all output directories if they don't exist."""
        for d in [
            self.output_dir,
            f"{self.output_dir}/bronze",
            f"{self.output_dir}/silver",
            f"{self.output_dir}/gold",
            self.logs_dir,
            self.checkpoint_dir,
            self.analytics_dir,
            self.recovered_dir,
        ]:
            Path(d).mkdir(parents=True, exist_ok=True)


@dataclass
class Config:
    """Root configuration object. Access via module-level `cfg` singleton."""
    db:       DatabaseConfig = field(default_factory=DatabaseConfig)
    api:      APIConfig      = field(default_factory=APIConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)

    @classmethod
    def from_json(cls, path: str) -> "Config":
        """
        Load config from a JSON file (overrides env var defaults).

        Args:
            path: Path to config.json file

        Returns:
            Config instance with values from JSON
        """
        with open(path) as f:
            data = json.load(f)

        cfg = cls()
        for section, values in data.items():
            if hasattr(cfg, section) and isinstance(values, dict):
                obj = getattr(cfg, section)
                for k, v in values.items():
                    if hasattr(obj, k):
                        setattr(obj, k, v)
        return cfg


# Module-level singleton — import this directly
cfg = Config()
