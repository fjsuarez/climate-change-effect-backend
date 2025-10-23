"""
Data migration script: Load B-spline coefficients and temperature distribution data to PostgreSQL
Usage: python load_bspline_data.py

This follows the same pattern as load_data_to_db.py using SQLAlchemy and pandas.
"""

import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os

# Load environment variables from .env
load_dotenv()

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Construct the SQLAlchemy connection string
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"


def extract_country_code(urau_code: str) -> str:
    """Extract country code from URAU code (first 2 characters)"""
    return urau_code[:2].upper()


def load_urau_cities(engine):
    """
    Load unique URAU cities from coefs.csv and create city reference records
    Merge with URAU names from GeoJSON file
    """
    print("\n=== Step 1: Loading URAU cities ===")
    
    # Read coefficients CSV
    coefs_path = os.path.join("data", "coefs.csv")
    df_coefs = pd.read_csv(coefs_path)
    
    # Get unique URAU codes
    unique_cities = df_coefs['URAU_CODE'].unique()
    
    # Create DataFrame with city data
    df_cities = pd.DataFrame({
        'urau_code': unique_cities,
        'country_code': [extract_country_code(code) for code in unique_cities],
        'name': None  # Will be populated from GeoJSON
    })
    
    # Load URAU names from GeoJSON
    print("Loading URAU names from GeoJSON...")
    geojson_path = os.path.join("data", "URAU_RG_100K_2021_3035.geojson")
    
    try:
        # Read GeoJSON file using geopandas
        gdf = gpd.read_file(geojson_path)
        
        # Extract URAU_CODE and URAU_NAME from properties
        df_names = gdf[['URAU_CODE', 'URAU_NAME']].copy()
        df_names.columns = ['urau_code', 'name']
        
        # Merge names with city data
        df_cities = df_cities.merge(df_names, on='urau_code', how='left', suffixes=('', '_geojson'))
        
        # Use the name from GeoJSON if available
        df_cities['name'] = df_cities['name_geojson'].fillna(df_cities['name'])
        df_cities = df_cities.drop(columns=['name_geojson'], errors='ignore')
        
        names_loaded = df_cities['name'].notna().sum()
        print(f"✓ Loaded {names_loaded} URAU names from GeoJSON")
        
    except Exception as e:
        print(f"Warning: Could not load URAU names from GeoJSON: {e}")
        print("Cities will be created without names")
    
    print(f"Found {len(df_cities)} unique URAU cities")
    
    # Drop dependent tables first, then parent table
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS temperature_distribution CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS bspline_coefficients CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS urau_cities CASCADE"))
        conn.commit()
    
    # Write to database
    df_cities.to_sql(
        'urau_cities',
        engine,
        if_exists='fail',  # Should not exist after DROP
        index=False
    )
    
    print(f"✓ Successfully inserted {len(df_cities)} URAU cities")
    return len(df_cities)


def load_bspline_coefficients(engine):
    """Load B-spline coefficients from coefs.csv to PostgreSQL"""
    print("\n=== Step 2: Loading B-spline coefficients ===")
    
    # Read coefficients CSV
    coefs_path = os.path.join("data", "coefs.csv")
    df_coefs = pd.read_csv(coefs_path)
    
    print(f"Found {len(df_coefs)} coefficient records")
    
    # Rename columns to match database schema
    df_coefs = df_coefs.rename(columns={'URAU_CODE': 'urau_code'})
    
    # Write to database
    df_coefs.to_sql(
        'bspline_coefficients',
        engine,
        if_exists='fail',  # Should not exist after DROP in step 1
        index=False
    )
    
    print(f"✓ Successfully inserted {len(df_coefs)} B-spline coefficient records")
    return len(df_coefs)


def load_temperature_distribution(engine):
    """Load temperature distribution data from tmean_distribution.csv to PostgreSQL"""
    print("\n=== Step 3: Loading temperature distribution data ===")
    
    # Read temperature distribution CSV
    dist_path = os.path.join("data", "tmean_distribution.csv")
    df_dist = pd.read_csv(dist_path)
    
    # Transform from wide format (columns p0, p1, ..., p100) to long format
    # Each row becomes 101 rows (one for each percentile)
    records = []
    
    for _, row in df_dist.iterrows():
        urau_code = row['URAU_CODE']
        # Each column represents a percentile (e.g., "0.0%", "1.0%", etc.)
        for col in df_dist.columns:
            if col != 'URAU_CODE' and '%' in col:
                # Extract percentile number from column name (e.g., "10.0%" -> 10)
                percentile = int(float(col.replace('%', '')))
                temperature = row[col]
                
                if pd.notna(temperature):  # Only add if temperature value exists
                    records.append({
                        'urau_code': urau_code,
                        'percentile': percentile,
                        'temperature': float(temperature)
                    })
    
    # Create DataFrame from records
    df_temp_dist = pd.DataFrame(records)
    
    print(f"Found {len(df_temp_dist)} temperature distribution records")
    
    # Write to database
    df_temp_dist.to_sql(
        'temperature_distribution',
        engine,
        if_exists='fail',  # Should not exist after DROP in step 1
        index=False
    )
    
    print(f"✓ Successfully inserted {len(df_temp_dist)} temperature distribution records")
    return len(df_temp_dist)


def verify_data(engine):
    """Verify that data was loaded correctly"""
    print("\n=== Step 4: Verifying data ===")
    
    with engine.connect() as conn:
        # Count cities
        result = conn.execute(text("SELECT COUNT(*) FROM urau_cities"))
        cities_count = result.scalar()
        print(f"✓ URAU cities: {cities_count}")
        
        # Count coefficients
        result = conn.execute(text("SELECT COUNT(*) FROM bspline_coefficients"))
        coefs_count = result.scalar()
        print(f"✓ B-spline coefficients: {coefs_count}")
        
        # Count temperature distributions
        result = conn.execute(text("SELECT COUNT(*) FROM temperature_distribution"))
        temp_count = result.scalar()
        print(f"✓ Temperature distributions: {temp_count}")
        
        # Sample query: Get coefficients for one city
        result = conn.execute(
            text("SELECT * FROM bspline_coefficients WHERE urau_code = 'AT001C'")
        )
        sample = result.fetchall()
        if sample:
            print(f"✓ Sample query successful - found {len(sample)} age groups for AT001C")


def main():
    """Main migration function"""
    print("=" * 60)
    print("B-SPLINE DATA MIGRATION TO POSTGRESQL")
    print("=" * 60)
    
    try:
        # Create the SQLAlchemy engine
        engine = create_engine(DATABASE_URL)
        print("✓ Database engine created successfully")
        
        # Step 1: Load URAU cities
        cities_count = load_urau_cities(engine)
        
        # Step 2: Load B-spline coefficients
        coefs_count = load_bspline_coefficients(engine)
        
        # Step 3: Load temperature distribution
        temp_count = load_temperature_distribution(engine)
        
        # Step 4: Verify data
        verify_data(engine)
        
        print("\n" + "=" * 60)
        print("✓ MIGRATION COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("Summary:")
        print(f"  - {cities_count} URAU cities")
        print(f"  - {coefs_count} B-spline coefficients")
        print(f"  - {temp_count} temperature distribution records")
        print("=" * 60)
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ MIGRATION FAILED: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
