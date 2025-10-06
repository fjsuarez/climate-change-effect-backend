from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import Response
from fastapi.middleware.gzip import GZipMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import geopandas as gpd
import os
from dotenv import load_dotenv

load_dotenv()

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Construct the SQLAlchemy connection string
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

app = FastAPI()

app.add_middleware(GZipMiddleware, minimum_size=1000)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    return {"message": "Welcome to the Climate Change Effect API!"}

@app.get("/health-check")
def read_health():
    return {"status": "healthy"}

@app.get("/api/v1/regions")
def get_regions(tolerance: float = 0.005, db: Session = Depends(get_db)):
    """
    Returns a GeoJSON FeatureCollection of all regions.
    """
    print(f"Fetching regions with simplification tolerance: {tolerance}...")
    try:
        # Use GeoPandas to read data from PostGIS
        # This is the easiest way to get GeoJSON output
        sql_query = """
                        SELECT "NUTS_ID", "name", ST_Simplify(geometry, %s) AS geometry
                        FROM regions
                        """
        gdf = gpd.read_postgis(sql_query, con=engine, geom_col='geometry', params=(tolerance,))

        # Convert to GeoJSON
        geojson_data = gdf.to_json()

        # TODO: In production, we might want to return a StreamingResponse
        # for large GeoJSON files, but for now this is fine.
        
        # Returning raw JSON string requires a custom response in FastAPI
        # A simpler way is to convert to a dict and let FastAPI handle it.
        # return gdf.to_dict(orient="records") # This is not GeoJSON, let's fix this.
        # The above is not correct for GeoJSON. Let's return the raw JSON string with a proper response class.
        print(geojson_data)
        return Response(content=geojson_data, media_type="application/json")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail="Error fetching regions from database")

@app.get("/api/v1/metrics/snapshot")
def get_metrics_snapshot(year: int, week: int, metric: str, db: Session = Depends(get_db)):
    """
    Returns a snapshot of a single metric for a given year and week.
    """
    print(f"Fetching snapshot for {metric} in {year}-W{week}...")
    # Basic validation to prevent SQL injection
    # In a real app, you'd have a predefined list of allowed metrics.
    if not metric.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid metric name.")
    query = text(f"""
    SELECT "NUTS_ID", "{metric}"
    FROM climate_data
    WHERE year = :year AND week = :week AND "{metric}" IS NOT NULL
""")
    try:
        result = db.execute(query, {"year": year, "week": week}).fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="No data found for the specified criteria.")
            # Convert the result to a dictionary { "NUTS_ID": value, ... }
        data = {row[0]: row[1] for row in result}
        return data
    except Exception as e:
        # This will catch errors if the metric column doesn't exist
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching data for metric '{metric}'.")