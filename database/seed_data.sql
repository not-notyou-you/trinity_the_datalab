
INSERT INTO regions_of_interest
    (region_code, name, description, bbox, area_km2, admin_level, country_code, is_active, source)
VALUES (
    'JABODTK',
    'Jabodetabek',
    'Jakarta-Bogor-Depok-Tangerang-Bekasi metropolitan area — primary flood monitoring zone',
    ST_GeomFromText('POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))', 4326),
    6392.0,
    2,
    'ID',
    TRUE,
    'SEEDER'
) ON CONFLICT (region_code) DO NOTHING;

INSERT INTO regions_of_interest
    (region_code, name, description, bbox, area_km2, admin_level, country_code, is_active, source)
VALUES (
    'JKT',
    'DKI Jakarta',
    'Special Capital Region of Jakarta — highest flood risk density',
    ST_GeomFromText('POLYGON((106.68 -6.37, 106.97 -6.37, 106.97 -6.07, 106.68 -6.07, 106.68 -6.37))', 4326),
    661.5,
    3,
    'ID',
    TRUE,
    'SEEDER'
) ON CONFLICT (region_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 2. Sample satellite scene (January 2024 flood event)
-- ---------------------------------------------------------------------------
INSERT INTO satellite_scenes (
    product_identifier,
    platform,
    instrument_mode,
    polarization_vv,
    polarization_vh,
    acquisition_datetime,
    orbit_number,
    orbit_direction,
    relative_orbit,
    bbox,
    cloud_cover_percent,
    incidence_angle_near,
    incidence_angle_far,
    resolution_m,
    region_id,
    raw_file_path,
    raw_file_size_mb,
    is_available
)
SELECT
    'S1A_IW_GRDH_1SDV_20240115T225041_20240115T225106_052186_064F3A_B5C2',
    'SENTINEL-1',
    'IW',
    TRUE,
    TRUE,
    '2024-01-15 22:50:41+00'::TIMESTAMPTZ,
    52186,
    'ASCENDING',
    98,
    ST_GeomFromText('POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))', 4326),
    12.5,
    30.8,
    46.2,
    10,
    r.region_id,
    '/data/raw/S1A_IW_GRDH_20240115.zip',
    847.3,
    TRUE
FROM regions_of_interest r
WHERE r.region_code = 'JABODTK'
ON CONFLICT (product_identifier) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 3. Verify inserts
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    scene_count INT;
    roi_count   INT;
BEGIN
    SELECT COUNT(*) INTO roi_count   FROM regions_of_interest;
    SELECT COUNT(*) INTO scene_count FROM satellite_scenes;
    RAISE NOTICE 'Seed complete: % regions, % scenes', roi_count, scene_count;
END $$;
