"""
Data loading script: Compute and load temperature histogram data to PostgreSQL
Usage: python load_histogram_data.py

This script:
1. Reads era5series.csv with daily temperature data (1990-2019)
2. Computes histograms for each city with different bin configurations (20, 30, 50 bins)
3. Loads the pre-computed histograms into the temperature_histogram table
"""

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os

try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm is not installed
    def tqdm(iterable, desc=None):
        print(f"{desc}..." if desc else "Processing...")
        return iterable

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


def load_temperature_histograms(engine):
    """
    Compute and load temperature histograms for all cities.
    Creates histograms with 20, 30, and 50 bins for each city.
    """
    print("\n=== Computing Temperature Histograms ===")
    
    # Read ERA5 series data
    era5_path = os.path.join("data", "era5series.csv")
    print(f"Loading ERA5 data from {era5_path}...")
    df_era5 = pd.read_csv(era5_path)
    print(f"Loaded {len(df_era5):,} daily temperature records")
    
    # Read temperature distribution for percentile ranges
    dist_path = os.path.join("data", "tmean_distribution.csv")
    print(f"Loading temperature distribution from {dist_path}...")
    df_dist = pd.read_csv(dist_path)
    print(f"Loaded distribution data for {len(df_dist)} cities")
    
    # Get unique cities
    unique_cities = df_era5['URAU_CODE'].unique()
    print(f"Found {len(unique_cities)} unique cities")
    
    # Bin configurations to compute
    bin_configs = [20, 30, 50]
    
    # Prepare data for bulk insert
    histogram_records = []
    
    print("\nComputing histograms for each city...")
    for urau_code in tqdm(unique_cities, desc="Processing cities"):
        # Get temperature data for this city
        city_temps = df_era5[df_era5['URAU_CODE'] == urau_code]['era5landtmean'].values
        
        # Get percentile range (1st to 99th percentile)
        city_dist = df_dist[df_dist['URAU_CODE'] == urau_code]
        
        if city_dist.empty:
            print(f"Warning: No distribution data found for {urau_code}, skipping...")
            continue
        
        p1_temp = city_dist['1.0%'].values[0]
        p99_temp = city_dist['99.0%'].values[0]
        
        if len(city_temps) == 0:
            print(f"Warning: No temperatures found for {urau_code}, skipping...")
            continue
        
        # Compute histograms for each bin configuration
        for bins_total in bin_configs:
            # Separate temps into: below p1, p1-p99, above p99
            temps_below = city_temps[city_temps < p1_temp]
            temps_main = city_temps[(city_temps >= p1_temp) & (city_temps <= p99_temp)]
            temps_above = city_temps[city_temps > p99_temp]
            
            # Create main histogram for p1-p99 range
            counts, bin_edges = np.histogram(temps_main, bins=bins_total, range=(p1_temp, p99_temp))
            
            # Add extreme bins if they contain data
            # Lower extreme bin (< 1st percentile)
            if len(temps_below) > 0:
                min_temp = float(temps_below.min())
                histogram_records.append({
                    'urau_code': urau_code,
                    'bin_start': min_temp,
                    'bin_end': p1_temp,
                    'bin_center': (min_temp + p1_temp) / 2,
                    'count': int(len(temps_below)),
                    'bins_total': bins_total
                })
            
            # Create records for main bins (p1-p99)
            for i in range(len(counts)):
                bin_start = float(bin_edges[i])
                bin_end = float(bin_edges[i + 1])
                bin_center = float((bin_start + bin_end) / 2)
                count = int(counts[i])
                
                histogram_records.append({
                    'urau_code': urau_code,
                    'bin_start': bin_start,
                    'bin_end': bin_end,
                    'bin_center': bin_center,
                    'count': count,
                    'bins_total': bins_total
                })
            
            # Upper extreme bin (> 99th percentile)
            if len(temps_above) > 0:
                max_temp = float(temps_above.max())
                histogram_records.append({
                    'urau_code': urau_code,
                    'bin_start': p99_temp,
                    'bin_end': max_temp,
                    'bin_center': (p99_temp + max_temp) / 2,
                    'count': int(len(temps_above)),
                    'bins_total': bins_total
                })
    
    # Convert to DataFrame
    df_histograms = pd.DataFrame(histogram_records)
    print(f"\nComputed {len(df_histograms):,} histogram bins")
    print(f"  - {len(unique_cities)} cities × {len(bin_configs)} configurations")
    print(f"  - Average {len(df_histograms) / len(unique_cities) / len(bin_configs):.0f} bins per configuration")
    
    # Load to database
    print("\nLoading histogram data to database...")
    
    # Clear existing data
    with engine.connect() as conn:
        result = conn.execute(text("DELETE FROM temperature_histogram"))
        conn.commit()
        print(f"Cleared {result.rowcount} existing records")
    
    # Insert new data
    df_histograms.to_sql(
        'temperature_histogram',
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"✓ Successfully loaded {len(df_histograms):,} histogram records")
    
    # Verify data
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                bins_total,
                COUNT(*) as num_records,
                COUNT(DISTINCT urau_code) as num_cities
            FROM temperature_histogram
            GROUP BY bins_total
            ORDER BY bins_total
        """))
        
        print("\nVerification - Records by bin configuration:")
        for row in result:
            print(f"  {row.bins_total} bins: {row.num_records:,} records across {row.num_cities} cities")


def main():
    """Main execution"""
    print("Starting temperature histogram data loading...")
    print(f"Database: {DBNAME} @ {HOST}")
    
    # Create engine
    engine = create_engine(DATABASE_URL)
    
    try:
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            row = result.fetchone()
            if row:
                version = row[0]
                print(f"✓ Connected to PostgreSQL: {version[:50]}...")
            else:
                print("✓ Connected to PostgreSQL")
        
        # Load histogram data
        load_temperature_histograms(engine)
        
        print("\n✅ All data loaded successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
