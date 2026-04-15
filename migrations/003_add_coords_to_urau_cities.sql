-- Migration: Add coordinates to urau_cities table
-- Description: Add lon/lat columns so the /api/v1/cities/with-erf endpoint
--              can serve map coordinates from the database instead of a local file.

ALTER TABLE urau_cities ADD COLUMN IF NOT EXISTS lon double precision;
ALTER TABLE urau_cities ADD COLUMN IF NOT EXISTS lat double precision;

COMMENT ON COLUMN urau_cities.lon IS 'WGS84 longitude of city centroid (EPSG:4326)';
COMMENT ON COLUMN urau_cities.lat IS 'WGS84 latitude of city centroid (EPSG:4326)';
