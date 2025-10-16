from pydantic import BaseModel
from typing import List, Optional

# Pydantic Models (Data Shapes)
# This defines the structure for a single data point in the time-series
class TimeSeriesDataPoint(BaseModel):
   year: int
   week: int
   metric1_value: float
   metric2_value: Optional[float] = None # Use Optional for the second metric

# This defines the overall shape of the API response
class TimeSeriesResponse(BaseModel):
   nuts_id: str
   metric1: str
   metric2: Optional[str] = None
   data: List[TimeSeriesDataPoint]
   # This is for Pydantic V1. If you are on V2 (likely with a new install),
   # you don't need this. For compatibility, it's good to have.
   class Config:
    from_attributes = True # orm_mode = True in Pydantic v1