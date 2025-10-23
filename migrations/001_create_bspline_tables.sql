-- Migration: Create B-spline and temperature distribution tables
-- Created: 2025-10-23
-- Description: Tables for storing URAU city data, B-spline coefficients, and temperature distributions

-- Table for URAU cities (reference table)
CREATE TABLE IF NOT EXISTS urau_cities (
  urau_code text PRIMARY KEY,
  country_code text NOT NULL,
  name text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Index for NUTS-to-URAU mapping (country code filtering)
CREATE INDEX IF NOT EXISTS idx_urau_country ON urau_cities(country_code);

-- Table for B-spline coefficients
CREATE TABLE IF NOT EXISTS bspline_coefficients (
  id bigserial PRIMARY KEY,
  urau_code text NOT NULL,
  agegroup text NOT NULL,
  b1 double precision NOT NULL,
  b2 double precision NOT NULL,
  b3 double precision NOT NULL,
  b4 double precision NOT NULL,
  b5 double precision NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(urau_code, agegroup)
);

-- Index for faster lookups by URAU code and age group
CREATE INDEX IF NOT EXISTS idx_bspline_urau_age ON bspline_coefficients(urau_code, agegroup);

-- Table for temperature distribution percentiles
CREATE TABLE IF NOT EXISTS temperature_distribution (
  id bigserial PRIMARY KEY,
  urau_code text NOT NULL,
  percentile smallint NOT NULL CHECK (percentile >= 0 AND percentile <= 100),
  temperature double precision NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(urau_code, percentile)
);

-- Index for faster lookups by URAU code and percentile
CREATE INDEX IF NOT EXISTS idx_temp_dist_urau_percentile ON temperature_distribution(urau_code, percentile);

-- Add foreign key constraints for data integrity
ALTER TABLE bspline_coefficients 
  DROP CONSTRAINT IF EXISTS fk_bspline_urau_code;
  
ALTER TABLE bspline_coefficients 
  ADD CONSTRAINT fk_bspline_urau_code 
  FOREIGN KEY (urau_code) 
  REFERENCES urau_cities(urau_code)
  ON DELETE CASCADE;

ALTER TABLE temperature_distribution 
  DROP CONSTRAINT IF EXISTS fk_temp_dist_urau_code;

ALTER TABLE temperature_distribution 
  ADD CONSTRAINT fk_temp_dist_urau_code 
  FOREIGN KEY (urau_code) 
  REFERENCES urau_cities(urau_code)
  ON DELETE CASCADE;

-- Add comments for documentation
COMMENT ON TABLE urau_cities IS 'Reference table for URAU (Urban Audit) cities';
COMMENT ON TABLE bspline_coefficients IS 'B-spline coefficients for temperature-mortality exposure-response functions by city and age group';
COMMENT ON TABLE temperature_distribution IS 'Temperature distribution percentiles for each URAU city';

COMMENT ON COLUMN bspline_coefficients.agegroup IS 'Age group: 20-44, 45-64, 65-74, 75-84, or 85+';
COMMENT ON COLUMN bspline_coefficients.b1 IS 'B-spline coefficient 1 (quadratic spline)';
COMMENT ON COLUMN bspline_coefficients.b2 IS 'B-spline coefficient 2 (quadratic spline)';
COMMENT ON COLUMN bspline_coefficients.b3 IS 'B-spline coefficient 3 (quadratic spline)';
COMMENT ON COLUMN bspline_coefficients.b4 IS 'B-spline coefficient 4 (quadratic spline)';
COMMENT ON COLUMN bspline_coefficients.b5 IS 'B-spline coefficient 5 (quadratic spline)';
COMMENT ON COLUMN temperature_distribution.percentile IS 'Temperature percentile (0-100)';
COMMENT ON COLUMN temperature_distribution.temperature IS 'Temperature value in Celsius at this percentile';
