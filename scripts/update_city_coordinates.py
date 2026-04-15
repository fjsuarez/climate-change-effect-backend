"""
Script: Populate lon/lat coordinates in the urau_cities table.

Run AFTER migration 003_add_coords_to_urau_cities.sql.

Strategy:
  1. Primary source: URAU_RG_100K_2021_3035.geojson (polygon file, EPSG:3035)
     - Compute polygon centroids, reproject to WGS84 → covers 713 cities
  2. Supplement: URAU_LB_2018_4326.geojson (label-point file, already WGS84)
     - Downloaded from Eurostat GISCO to cover UK cities absent from 2021 RG file
     - 2018 codes use trailing digit (e.g. UK001C1) → strip to match 2021 format

Usage:
  cd climate-change-effect-backend
  python scripts/update_city_coordinates.py
"""

import os
import re
import csv
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
RG_FILE = os.path.join(DATA_DIR, "URAU_RG_100K_2021_3035.geojson")
LB_2018_URL = "https://gisco-services.ec.europa.eu/distribution/v2/urau/geojson/URAU_LB_2018_4326.geojson"
LB_CACHE = os.path.join(DATA_DIR, "URAU_LB_2018_4326_temp.geojson")


def load_expected_codes() -> set:
    coefs_path = os.path.join(DATA_DIR, "coefs.csv")
    with open(coefs_path) as f:
        return set(r["URAU_CODE"] for r in csv.DictReader(f))


def coords_from_rg_file(expected_codes: set) -> dict:
    """Compute WGS84 centroids from the polygon RG file."""
    print(f"Reading {RG_FILE} ...")
    gdf = gpd.read_file(RG_FILE)
    gdf = gdf[gdf["URAU_CODE"].isin(expected_codes)].copy()
    gdf["centroid"] = gdf.geometry.centroid
    gdf_wgs = gdf.set_geometry("centroid").to_crs("EPSG:4326")
    coords = {
        row["URAU_CODE"]: (row["centroid"].x, row["centroid"].y)
        for _, row in gdf_wgs.iterrows()
    }
    print(f"  → {len(coords)} cities from RG file")
    return coords


def coords_from_lb_2018(missing_codes: set) -> dict:
    """
    Fetch 2018 label-point file to cover UK/LV codes absent from 2021 RG file.
    2018 codes have trailing digit suffix (UK001C1) — strip it to match current format.
    """
    import urllib.request

    if not os.path.exists(LB_CACHE):
        print(f"Downloading URAU LB 2018 from Eurostat...")
        urllib.request.urlretrieve(LB_2018_URL, LB_CACHE)
        print(f"  → Downloaded to {LB_CACHE}")
    else:
        print(f"  Using cached {LB_CACHE}")

    gdf = gpd.read_file(LB_CACHE)
    coords = {}
    for _, row in gdf.iterrows():
        raw_code = row.get("URAU_CODE") or row.get("urau_code") or ""
        # Normalise: strip trailing digit (UK001C1 → UK001C)
        normalised = re.sub(r"\d+$", "", raw_code)
        if normalised in missing_codes:
            coords[normalised] = (row.geometry.x, row.geometry.y)

    print(f"  → {len(coords)} additional cities from LB 2018")
    return coords


def update_db(coords: dict):
    engine = create_engine(DATABASE_URL)
    updated = 0
    with engine.connect() as conn:
        for urau_code, (lon, lat) in coords.items():
            result = conn.execute(
                text(
                    "UPDATE urau_cities SET lon = :lon, lat = :lat WHERE urau_code = :code"
                ),
                {"lon": lon, "lat": lat, "code": urau_code},
            )
            updated += result.rowcount
        conn.commit()
    print(f"  → Updated {updated} rows in urau_cities")
    return updated


def main():
    expected_codes = load_expected_codes()
    print(f"Expected cities (from coefs.csv): {len(expected_codes)}")

    # Step 1: primary source
    rg_coords = coords_from_rg_file(expected_codes)
    covered = set(rg_coords)

    # Step 2: supplement with 2018 LB for missing codes
    missing = expected_codes - covered
    print(f"Still missing after RG file: {len(missing)} — fetching LB 2018 for supplement")
    lb_coords = coords_from_lb_2018(missing)

    # Merge (primary wins)
    all_coords = {**lb_coords, **rg_coords}
    covered_total = len(all_coords)
    still_missing = expected_codes - set(all_coords)
    print(f"\nTotal coords resolved: {covered_total} / {len(expected_codes)}")
    if still_missing:
        from collections import Counter
        print(f"Still missing: {len(still_missing)} — {sorted(Counter(c[:2] for c in still_missing).items())}")

    # Step 3: push to DB
    print("\nUpdating urau_cities in database...")
    update_db(all_coords)

    # Step 4: clean up temp file
    if os.path.exists(LB_CACHE):
        os.remove(LB_CACHE)
        print("  Cleaned up temp LB file")

    print("\nDone.")


if __name__ == "__main__":
    main()
