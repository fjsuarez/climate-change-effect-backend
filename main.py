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
DB_PORT = os.getenv("port")  # Renamed to avoid confusion with Railway PORT
DBNAME = os.getenv("dbname")

# Construct the SQLAlchemy connection string
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{DB_PORT}/{DBNAME}?sslmode=require"

app = FastAPI()

# Add CORS middleware - MUST be before other middleware
# Allow localhost for development and production URLs
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:3000,http://localhost:3001"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
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
        raise HTTPException(status_code=500, detail="Error fetching data from database")


# --- B-Spline Coefficients Endpoint ---
@app.get("/api/v1/coefficients")
def get_coefficients():
    """
    Returns B-spline coefficients from coefs.csv.
    Returns a list of all coefficients with URAU_CODE, agegroup, and b1-b5 values.
    """
    import csv
    import os
    
    print("Fetching B-spline coefficients...")
    
    csv_path = os.path.join(os.path.dirname(__file__), "data", "coefs.csv")
    
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="Coefficients file not found")
    
    try:
        coefficients = []
        with open(csv_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                coefficients.append({
                    "urau_code": row["URAU_CODE"],
                    "agegroup": row["agegroup"],
                    "b1": float(row["b1"]),
                    "b2": float(row["b2"]),
                    "b3": float(row["b3"]),
                    "b4": float(row["b4"]),
                    "b5": float(row["b5"])
                })
        
        return coefficients
    
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error reading coefficients file")


@app.get("/api/v1/coefficients/distribution")
def get_temperature_distribution():
    """
    Returns temperature distribution percentiles for each city from tmean_distribution.csv.
    """
    import csv
    import os
    
    print("Fetching temperature distribution...")
    
    csv_path = os.path.join(os.path.dirname(__file__), "data", "tmean_distribution.csv")
    
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="Distribution file not found")
    
    try:
        distributions = []
        with open(csv_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Extract key percentiles needed for B-spline knots
                distributions.append({
                    "urau_code": row["URAU_CODE"],
                    "p10": float(row["10.0%"]),
                    "p75": float(row["75.0%"]),
                    "p90": float(row["90.0%"]),
                    # Also include min/max for plotting range
                    "min": float(row["0.0%"]),
                    "max": float(row["100.0%"])
                })
        
        return distributions
    
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error reading distribution file")


@app.get("/api/v1/bspline/evaluate")
def evaluate_bspline(urau_code: str, agegroup: str, db: Session = Depends(get_db)):
    """
    Evaluates the B-spline exposure-response curve for a specific city and age group.
    
    This implements the methodology from the European temperature-mortality study:
    1. Creates a natural quadratic B-spline with knots at 10th, 75th, and 90th percentiles
    2. Computes log relative risk across temperature percentiles
    3. Identifies MMT (Minimum Mortality Temperature) within 25th-75th percentile range
    4. Centers the curve at MMT (RR = 1 at MMT)
    5. Returns temperature points and relative risk values
    """
    import numpy as np
    
    print(f"Evaluating B-spline for {urau_code}, age group {agegroup}...")
    
    try:
        # Load coefficients from database
        coef_query = text("""
            SELECT b1, b2, b3, b4, b5 
            FROM bspline_coefficients 
            WHERE urau_code = :urau_code AND agegroup = :agegroup
        """)
        coef_result = db.execute(coef_query, {"urau_code": urau_code, "agegroup": agegroup}).first()
        
        if coef_result is None:
            raise HTTPException(status_code=404, detail=f"No coefficients found for {urau_code}, {agegroup}")
        
        coefficients = np.array([coef_result.b1, coef_result.b2, coef_result.b3, coef_result.b4, coef_result.b5])
        
        # Load full temperature distribution (all percentiles) from database
        dist_query = text("""
            SELECT percentile, temperature 
            FROM temperature_distribution 
            WHERE urau_code = :urau_code
            ORDER BY percentile
        """)
        dist_result = db.execute(dist_query, {"urau_code": urau_code}).fetchall()
        
        if not dist_result:
            raise HTTPException(status_code=404, detail=f"No distribution found for {urau_code}")
        
        # Convert to dictionary and arrays
        temp_percentiles = {row.percentile: row.temperature for row in dist_result}
        percentile_values = np.array([row.percentile for row in dist_result])
        temperatures = np.array([row.temperature for row in dist_result])
        
        # Get key percentiles for knots
        p10_temp = temp_percentiles.get(10)
        p75_temp = temp_percentiles.get(75)
        p90_temp = temp_percentiles.get(90)
        p25_temp = temp_percentiles.get(25)  # For MMT range constraint
        
        if None in [p10_temp, p75_temp, p90_temp, p25_temp]:
            raise HTTPException(status_code=500, detail="Missing required percentiles in temperature distribution")
        
        # Build knot vector for natural quadratic B-spline (degree 2)
        # For 5 coefficients: need 8 knots total
        knots = np.array([
            temperatures.min(), temperatures.min(), temperatures.min(),  # Left boundary (3x)
            p10_temp, p75_temp, p90_temp,                                # Internal knots
            temperatures.max(), temperatures.max()                        # Right boundary (2x)
        ])
        
        # Create B-spline basis and evaluate at all temperature percentiles
        from scipy.interpolate import BSpline
        
        bspline = BSpline(knots, coefficients, 2)
        log_rr_all = bspline(temperatures)
        
        # Step 3: Identify MMT within 25th-75th percentile range
        # Create mask for acceptable range (25th-75th percentiles)
        in_range = (temperatures >= p25_temp) & (temperatures <= p75_temp)
        
        # Find MMT: temperature with minimum log RR within acceptable range
        valid_log_rr = log_rr_all[in_range]
        valid_temps = temperatures[in_range]
        
        if len(valid_log_rr) == 0:
            raise HTTPException(status_code=500, detail="No valid temperatures in MMT search range")
        
        mmt_idx = np.argmin(valid_log_rr)
        mmt = valid_temps[mmt_idx]
        log_rr_at_mmt = valid_log_rr[mmt_idx]
        
        # Find the percentile of MMT
        mmt_percentile = percentile_values[in_range][mmt_idx]
        
        # Step 4: Center the curve at MMT
        # Subtract the log RR at MMT so that RR = 1 (log RR = 0) at MMT
        log_rr_centered = log_rr_all - log_rr_at_mmt
        
        # Convert to relative risk: RR = exp(log_rr_centered)
        rr_centered = np.exp(log_rr_centered)
        
        # Generate smooth curve for plotting (100 points)
        temp_plot = np.linspace(temperatures.min(), temperatures.max(), 100)
        log_rr_plot = bspline(temp_plot) - log_rr_at_mmt
        rr_plot = np.exp(log_rr_plot)
        
        # Compute percentiles for plotting points by interpolation
        percentile_plot = np.interp(temp_plot, temperatures, percentile_values)
        
        # Extract extreme percentile values (1st and 99th)
        rr_at_p01 = rr_centered[percentile_values == 1][0] if 1 in percentile_values else None
        rr_at_p99 = rr_centered[percentile_values == 99][0] if 99 in percentile_values else None
        
        return {
            "urau_code": urau_code,
            "agegroup": agegroup,
            "knots": {
                "p10": float(p10_temp),
                "p75": float(p75_temp),
                "p90": float(p90_temp)
            },
            "mmt": {
                "temperature": float(mmt),
                "percentile": float(mmt_percentile),
                "relative_risk": 1.0  # By definition, RR = 1 at MMT
            },
            "extreme_rr": {
                "rr_at_p01": float(rr_at_p01) if rr_at_p01 is not None else None,
                "rr_at_p99": float(rr_at_p99) if rr_at_p99 is not None else None,
                "temp_at_p01": float(temp_percentiles.get(1, temperatures.min())),
                "temp_at_p99": float(temp_percentiles.get(99, temperatures.max()))
            },
            "data": [
                {
                    "temperature": float(t),
                    "percentile": float(p),
                    "value": float(rr)
                }
                for t, p, rr in zip(temp_plot, percentile_plot, rr_plot)
            ]
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error evaluating B-spline: {str(e)}")


@app.get("/api/v1/coefficients/cities")
def get_cities(db: Session = Depends(get_db)):
    """
    Returns a list of unique city codes (URAU_CODE) and names from the coefficients data.
    """
    print("Fetching unique city codes...")
    
    try:
        query = text("SELECT urau_code, name FROM urau_cities ORDER BY urau_code")
        result = db.execute(query).fetchall()
        
        cities = [{"code": row.urau_code, "name": row.name} for row in result]
        return {"cities": cities}
    
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail="Error fetching city codes from database")

@app.get("/api/v1/coefficients/cities/by-nuts/{nuts_id}")
def get_cities_by_nuts(nuts_id: str, db: Session = Depends(get_db)):
    """
    Returns a list of URAU codes and names that belong to a specific NUTS region.
    URAU codes start with the NUTS country code (e.g., AT001C is in Austria AT).
    """
    print(f"Fetching URAU codes for NUTS region {nuts_id}...")
    
    try:
        # Extract country code from NUTS_ID (first 2 characters)
        country_code = nuts_id[:2].upper()
        
        query = text("""
            SELECT urau_code, name 
            FROM urau_cities 
            WHERE country_code = :country_code 
            ORDER BY urau_code
        """)
        result = db.execute(query, {"country_code": country_code}).fetchall()
        
        cities = [{"code": row.urau_code, "name": row.name} for row in result]
        return {"nuts_id": nuts_id, "cities": cities}
    
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail="Error fetching city codes from database")

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