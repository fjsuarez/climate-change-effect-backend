-- Migration: Create temperature_histogram table
-- Description: Stores pre-computed temperature histogram bins for each URAU city
-- Date: 2025-11-08

CREATE TABLE IF NOT EXISTS temperature_histogram (
    id SERIAL PRIMARY KEY,
    urau_code VARCHAR(10) NOT NULL,
    bin_start FLOAT NOT NULL,
    bin_end FLOAT NOT NULL,
    bin_center FLOAT NOT NULL,
    count INTEGER NOT NULL,
    bins_total INTEGER NOT NULL,  -- Total number of bins (e.g., 20, 30, 50)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_histogram_bin UNIQUE(urau_code, bin_start, bins_total)
);

-- Create indexes for fast queries
CREATE INDEX idx_histogram_urau ON temperature_histogram(urau_code);
CREATE INDEX idx_histogram_urau_bins ON temperature_histogram(urau_code, bins_total);

-- Add comment
COMMENT ON TABLE temperature_histogram IS 'Pre-computed temperature histogram bins from ERA5 daily data (1990-2019)';
COMMENT ON COLUMN temperature_histogram.urau_code IS 'URAU city code';
COMMENT ON COLUMN temperature_histogram.bin_start IS 'Temperature bin start (°C)';
COMMENT ON COLUMN temperature_histogram.bin_end IS 'Temperature bin end (°C)';
COMMENT ON COLUMN temperature_histogram.bin_center IS 'Temperature bin center (°C)';
COMMENT ON COLUMN temperature_histogram.count IS 'Number of days in this temperature bin';
COMMENT ON COLUMN temperature_histogram.bins_total IS 'Total number of bins in this histogram (20, 30, or 50)';
