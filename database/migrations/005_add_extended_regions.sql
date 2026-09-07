-- database/migrations/005_add_extended_regions.sql
INSERT INTO regions_of_interest (region_code, name, description, bbox, area_km2, admin_level, country_code, is_active)
VALUES
    ('KOTA_BOGOR', 'Kota Bogor', 'Kota, sekitar 60 km selatan Jakarta',
     ST_GeomFromText('POLYGON((106.74 -6.65, 106.86 -6.65, 106.86 -6.52, 106.74 -6.52, 106.74 -6.65))', 4326),
     118.5, 3, 'ID', TRUE),
    ('KAB_BOGOR', 'Kabupaten Bogor', 'Meliputi Cibinong, Sentul, Ciawi',
     ST_GeomFromText('POLYGON((106.30 -6.90, 107.25 -6.90, 107.25 -6.20, 106.30 -6.20, 106.30 -6.90))', 4326),
     2710.0, 3, 'ID', TRUE),
    ('KOTA_DEPOK', 'Kota Depok', 'Selatan Jakarta, berbatasan langsung',
     ST_GeomFromText('POLYGON((106.71 -6.48, 106.90 -6.48, 106.90 -6.32, 106.71 -6.32, 106.71 -6.48))', 4326),
     200.3, 3, 'ID', TRUE),
    ('KOTA_TANGERANG', 'Kota Tangerang', 'Barat Jakarta, termasuk area bandara',
     ST_GeomFromText('POLYGON((106.56 -6.26, 106.72 -6.26, 106.72 -6.10, 106.56 -6.10, 106.56 -6.26))', 4326),
     164.5, 3, 'ID', TRUE),
    ('KAB_TANGERANG', 'Kabupaten Tangerang', 'Meliputi Serpong, BSD, Tangerang Selatan',
     ST_GeomFromText('POLYGON((106.45 -6.40, 106.80 -6.40, 106.80 -5.95, 106.45 -5.95, 106.45 -6.40))', 4326),
     959.6, 3, 'ID', TRUE),
    ('KOTA_BEKASI', 'Kota Bekasi', 'Timur Jakarta, wilayah paling padat',
     ST_GeomFromText('POLYGON((106.89 -6.35, 107.06 -6.35, 107.06 -6.13, 106.89 -6.13, 106.89 -6.35))', 4326),
     210.5, 3, 'ID', TRUE),
    ('KAB_BEKASI', 'Kabupaten Bekasi', 'Timur laut Jabodetabek, kawasan industri',
     ST_GeomFromText('POLYGON((106.95 -6.50, 107.45 -6.50, 107.45 -6.00, 106.95 -6.00, 106.95 -6.50))', 4326),
     1484.0, 3, 'ID', TRUE)
ON CONFLICT (region_code) DO NOTHING;

INSERT INTO schema_migrations (version, description)
VALUES ('005', 'Extended regions_of_interest with Jabodetabek sub-regions for the dataset creation location picker')
ON CONFLICT (version) DO NOTHING;