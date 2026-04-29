-- Migration: Create mortality_multiplier table
-- Description: Country-level mortality multipliers by RCP scenario, year, and age.
--              Values represent the ratio of projected mortality vs. baseline (1.0 = no change).
--              Decomposed into heat and cold components.

CREATE TABLE IF NOT EXISTS mortality_multiplier (
  id         bigserial PRIMARY KEY,
  country    text NOT NULL,
  rcp_scenario text NOT NULL,
  year       smallint NOT NULL,
  age        smallint NOT NULL,
  multiplier_total double precision NOT NULL,
  multiplier_heat  double precision NOT NULL,
  multiplier_cold  double precision NOT NULL,
  created_at timestamptz DEFAULT now(),
  UNIQUE (country, rcp_scenario, year, age)
);

CREATE INDEX IF NOT EXISTS idx_mm_country_rcp ON mortality_multiplier(country, rcp_scenario);
CREATE INDEX IF NOT EXISTS idx_mm_year ON mortality_multiplier(year);

COMMENT ON TABLE mortality_multiplier IS 'Country-level mortality multipliers by RCP scenario, year, and age';
COMMENT ON COLUMN mortality_multiplier.multiplier_total IS 'All-temperature mortality multiplier (1.0 = baseline)';
COMMENT ON COLUMN mortality_multiplier.multiplier_heat IS 'Heat-attributable component of the mortality multiplier';
COMMENT ON COLUMN mortality_multiplier.multiplier_cold IS 'Cold-attributable component of the mortality multiplier';
