# etl/constants.py
"""
Shared dedup-key constants for `nasa_scenes` rows.

The unique key is (source, tile_id, product_short_name, acquisition_date).
live_scheduler.py (live ingest) and module9_fusion.py (fusion build) must
both use these exact values, or they silently create duplicate/orphaned
rows for the same underlying scene instead of reusing one.
"""

MODIS_SOURCE = "MODIS"
MODIS_PRODUCT_SHORT_NAME = "MCDWD_L3_F2_NRT"
MODIS_TILE_ID = "MOSAIC"

GPM_SOURCE = "GPM"
GPM_PRODUCT_SHORT_NAME = "GPM_3IMERGDF"
GPM_TILE_ID = "GLOBAL"
