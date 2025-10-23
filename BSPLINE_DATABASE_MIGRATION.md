# B-Spline Data Migration Guide

## Overview

This document describes the migration of B-spline coefficients and temperature distribution data from CSV files to PostgreSQL database.

## Database Schema

### Tables Created

1. **urau_cities** - Reference table for URAU (Urban Audit) cities
   - `urau_code` (PK): City code (e.g., "AT001C")
   - `country_code`: 2-letter country code (e.g., "AT")
   - `name`: City name (optional, for future use)
   - `created_at`, `updated_at`: Timestamps

2. **bspline_coefficients** - B-spline coefficients for ERF curves
   - `id` (PK): Auto-increment ID
   - `urau_code` (FK): References urau_cities
   - `agegroup`: Age group (20-44, 45-64, 65-74, 75-84, 85+)
   - `b1, b2, b3, b4, b5`: Quadratic B-spline coefficients
   - `created_at`, `updated_at`: Timestamps
   - UNIQUE constraint on (urau_code, agegroup)

3. **temperature_distribution** - Temperature percentiles by city
   - `id` (PK): Auto-increment ID
   - `urau_code` (FK): References urau_cities
   - `percentile`: Integer 0-100
   - `temperature`: Temperature value in Celsius
   - `created_at`, `updated_at`: Timestamps
   - UNIQUE constraint on (urau_code, percentile)

### Indexes

- `idx_urau_country` on urau_cities(country_code) - for NUTS-to-URAU filtering
- `idx_bspline_urau_age` on bspline_coefficients(urau_code, agegroup) - for coefficient lookups
- `idx_temp_dist_urau_percentile` on temperature_distribution(urau_code, percentile) - for percentile lookups

## Migration Steps

### Step 1: Run SQL Migration

Execute the migration script to create the tables:

```bash
psql $DATABASE_URL -f migrations/001_create_bspline_tables.sql
```

Or use your PostgreSQL client (pgAdmin, DBeaver, etc.) to run the SQL script.

### Step 2: Load Data

Run the Python migration script to load data from CSV files:

```bash
cd climate-change-effect-backend
python scripts/load_bspline_data.py
```

The script will:
1. Extract unique URAU cities from `coefs.csv` and populate `urau_cities` table
2. Load all B-spline coefficients into `bspline_coefficients` table
3. Transform and load temperature distribution from wide format (percentile columns) to long format
4. Verify data integrity with sample queries

### Step 3: Verify Migration

Check the data was loaded correctly:

```sql
-- Count records
SELECT COUNT(*) FROM urau_cities;          -- Should show ~number of unique cities
SELECT COUNT(*) FROM bspline_coefficients; -- Should show cities × 5 age groups
SELECT COUNT(*) FROM temperature_distribution; -- Should show cities × 101 percentiles

-- Sample query
SELECT * FROM bspline_coefficients WHERE urau_code = 'AT001C';

-- Check NUTS-to-URAU mapping
SELECT * FROM urau_cities WHERE country_code = 'AT' ORDER BY urau_code;
```

## API Changes

### Before (CSV-based)

The following endpoints previously read from CSV files:

- `GET /api/v1/bspline/evaluate?urau_code=AT001C&agegroup=20-44`
- `GET /api/v1/coefficients/cities`
- `GET /api/v1/coefficients/cities/by-nuts/{nuts_id}`

### After (Database-based)

Same endpoints now use PostgreSQL queries:

**evaluate_bspline:**
```python
# Load coefficients
coef_query = text("""
    SELECT b1, b2, b3, b4, b5 
    FROM bspline_coefficients 
    WHERE urau_code = :urau_code AND agegroup = :agegroup
""")

# Load temperature distribution
dist_query = text("""
    SELECT percentile, temperature 
    FROM temperature_distribution 
    WHERE urau_code = :urau_code
    ORDER BY percentile
""")
```

**get_cities:**
```python
query = text("SELECT DISTINCT urau_code FROM urau_cities ORDER BY urau_code")
```

**get_cities_by_nuts:**
```python
query = text("""
    SELECT urau_code 
    FROM urau_cities 
    WHERE country_code = :country_code 
    ORDER BY urau_code
""")
```

## Benefits

### Performance
- ✅ Indexed lookups much faster than CSV scanning
- ✅ Database caching for frequently accessed data
- ✅ Optimized queries with proper indexes

### Scalability
- ✅ Can handle millions of records efficiently
- ✅ Concurrent access without file locking issues
- ✅ Easy to add new cities or update coefficients

### Maintainability
- ✅ Data integrity with foreign key constraints
- ✅ UNIQUE constraints prevent duplicates
- ✅ Timestamps track data changes
- ✅ Easy backup and restore

### Features
- ✅ Can add city names and metadata
- ✅ Support for versioning (future: add version column)
- ✅ Audit trail with timestamps
- ✅ Easy to query and analyze data

## Rollback

If needed, you can rollback by:

1. Reverting the API endpoints to use CSV files (restore from git)
2. Dropping the tables:

```sql
DROP TABLE IF EXISTS temperature_distribution CASCADE;
DROP TABLE IF EXISTS bspline_coefficients CASCADE;
DROP TABLE IF EXISTS urau_cities CASCADE;
```

## Future Enhancements

- Add city names to `urau_cities` table
- Add data versioning (version column + timestamp)
- Add confidence intervals for coefficients
- Add metadata (data source, methodology notes)
- Create views for common queries
- Add materialized views for expensive calculations
