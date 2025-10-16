from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import geopandas as gpd
import os
from dotenv import load_dotenv
from models.base import TimeSeriesDataPoint, TimeSeriesResponse
from typing import List, Optional

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

# Add CORS middleware - MUST be before other middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],  # Frontend URLs
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

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
    Returns a simple dictionary of {NUTS_ID: value} for a given metric/year/week.
    Frontend merges this with the region geometries.
    """
    print(f"Fetching snapshot for {metric} in {year}-W{week}...")
    # Basic validation to prevent SQL injection
    if not metric.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid metric name.")
    
    query = text(f"""
    SELECT "NUTS_ID", "{metric}" as value
    FROM climate_data
    WHERE year = :year AND week = :week AND "{metric}" IS NOT NULL
    """)
    
    try:
        result = db.execute(query, {"year": year, "week": week}).mappings().all()
        
        if not result:
            raise HTTPException(status_code=404, detail="No data found for the specified criteria.")
        
        # Return simple dict: {NUTS_ID: value}
        data = {row["NUTS_ID"]: float(row["value"]) for row in result}
        return data
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching data for metric '{metric}'.")

@app.get("/api/v1/metrics/range")
def get_metric_range(metric: str, db: Session = Depends(get_db)):
    """
    Returns the global min/max values for a metric across all time periods.
    Used to maintain consistent color scales.
    """
    print(f"Fetching global range for {metric}...")
    # Basic validation to prevent SQL injection
    if not metric.replace("_", "").isalnum():
        raise HTTPException(status_code=400, detail="Invalid metric name.")
    
    query = text(f"""
    SELECT 
        MIN("{metric}") as min_value,
        MAX("{metric}") as max_value
    FROM climate_data
    WHERE "{metric}" IS NOT NULL
    """)
    
    try:
        result = db.execute(query).mappings().first()
        
        if not result or result["min_value"] is None:
            raise HTTPException(status_code=404, detail=f"No data found for metric '{metric}'.")
        
        return {
            "metric": metric,
            "min_value": float(result["min_value"]),
            "max_value": float(result["max_value"])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching range for metric '{metric}'.")


# --- Time-Series Endpoint ---
@app.get("/api/v1/timeseries/{nuts_id}", response_model=TimeSeriesResponse)
def get_time_series(
    nuts_id: str,
    metric1: str,
    metric2: Optional[str] = None,
    db: Session = Depends(get_db)
   ):
    """
    Returns time-series data for one or two metrics for a specific NUTS region.
    """
    print(f"Fetching time-series for {nuts_id} with metrics: {metric1}, {metric2}")
    # --- Column validation to prevent SQL injection ---
    allowed_metrics = [
        "mortality_rate", "population_density", "population",
        "temp_era5_q05", "temp_era5_q50", "temp_era5_q95", 
        "temp_rcp45", "temp_rcp85", 
        "NOx", "O3", "pm10"
    ]  # Add all your valid metric columns here
    if metric1 not in allowed_metrics or (metric2 and metric2 not in allowed_metrics):
        raise HTTPException(status_code=400, detail="Invalid metric name specified.")
    
    # --- Build the query ---
    columns_to_select = f'"NUTS_ID", "year", "week", "{metric1}" as value'

    where_clause = f'"NUTS_ID" = :nuts_id AND "{metric1}" IS NOT NULL'
    
    if metric2:
        columns_to_select += f', "{metric2}" as value2'
        where_clause += f' AND "{metric2}" IS NOT NULL'

    query = text(f"""
        SELECT {columns_to_select}
        FROM climate_data
        WHERE {where_clause}
        ORDER BY year, week
        """)
    try:
        result = db.execute(query, {"nuts_id": nuts_id}).mappings().all()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"No data found for region {nuts_id}")
        
        # Transform to match frontend expectations
        data_points = []
        for row in result:
            point = {
                "year": row["year"],
                "week": row["week"],
                "metric1_value": float(row["value"]) if row["value"] is not None else None
            }
            if metric2 and "value2" in row:
                point["metric2_value"] = float(row["value2"]) if row["value2"] is not None else None
            data_points.append(point)

        return {
            "nuts_id": nuts_id, 
            "metric1": metric1,
            "metric2": metric2,
            "data": data_points
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error fetching time-series data: {str(e)}")