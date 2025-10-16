import psycopg2
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
from geoalchemy2 import Geometry

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

def main():
    # Create the SQLAlchemy engine    
    engine = create_engine(DATABASE_URL)
    print("Database engine created successfully.")

    # --- 2. Load Regions (Geospatial Data) ---
    print("Reading regions.geojson...")
    regions_path = os.path.join("data", "regions.geojson")
    gdf = gpd.read_file(regions_path)
    print("Writing geospatial data to 'regions' table...")
    
    # Ensure the GeoDataFrame has the correct CRS
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    
    # Convert geometry to WKT format for PostgreSQL compatibility
    gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt)
    
    gdf.to_sql(
        'regions',
        engine,
        if_exists='replace',
        index=False,
    )
    print("'regions' table created successfully.")
    
    # --- 3. Load Climate Data (Tabular Data) ---
    print("Reading climate data from CSV files...")
    df_eu = pd.read_csv(os.path.join("data", "europe.csv"))
    df_at = pd.read_csv(os.path.join("data", "austria.csv"))
    # You can combine them if they have the same structure
    combined_df = pd.concat([df_eu, df_at], ignore_index=True)
    print("Writing tabular data to 'climate_data' table...")
    # This will create a new table called 'climate_data'
    combined_df.to_sql(
        'climate_data',
        engine,
        if_exists='replace',
        index=False
    )
    print("'climate_data' table created successfully.")
    print("Database population complete!")

if __name__ == "__main__":
    main()