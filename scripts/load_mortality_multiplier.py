"""
Script: Load mortality multiplier data into the mortality_multiplier table.

Run AFTER migration 004_create_mortality_multiplier.sql has been applied in Supabase.

Usage:
  cd climate-change-effect-backend
  python scripts/load_mortality_multiplier.py
"""

import os
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

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "mortality_multiplier_country_rcp_year_age.csv")


def main():
    print("Loading mortality multiplier data...")
    df = pd.read_csv(DATA_FILE)
    print(f"  Rows: {len(df)}")
    print(f"  Countries: {sorted(df['country'].unique())}")
    print(f"  RCPs: {sorted(df['rcp_scenario'].unique())}")
    print(f"  Years: {df['year'].min()} – {df['year'].max()}")
    print(f"  Ages: {df['age'].min()} – {df['age'].max()}")

    # Rename columns to match DB schema
    df = df.rename(columns={"rcp_scenario": "rcp_scenario"})  # already correct

    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        existing = conn.execute(text("SELECT COUNT(*) FROM mortality_multiplier")).scalar()
        print(f"\n  Existing rows in DB: {existing}")
        if existing > 0:
            print("  Truncating existing data before reload...")
            conn.execute(text("TRUNCATE TABLE mortality_multiplier"))
            conn.commit()

    print("\nWriting to database (this may take a minute for ~300k rows)...")
    df.to_sql(
        "mortality_multiplier",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM mortality_multiplier")).scalar()
    print(f"\nDone. {count} rows in mortality_multiplier table.")


if __name__ == "__main__":
    main()
