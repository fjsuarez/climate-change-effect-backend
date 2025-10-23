"""
Test script for B-spline evaluation endpoint.
This script demonstrates the B-spline curve computation.
"""

import csv
import numpy as np
from scipy.interpolate import BSpline

# Load sample data
urau_code = "AT001C"
agegroup = "20-44"

# Load coefficients
with open('data/coefs.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        if row["URAU_CODE"] == urau_code and row["agegroup"] == agegroup:
            coefficients = [float(row[f"b{i}"]) for i in range(1, 6)]
            print(f"Coefficients for {urau_code}, {agegroup}: {coefficients}")
            break

# Load temperature distribution
with open('data/tmean_distribution.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        if row["URAU_CODE"] == urau_code:
            distribution = {
                "p10": float(row["10.0%"]),
                "p75": float(row["75.0%"]),
                "p90": float(row["90.0%"]),
                "min": float(row["0.0%"]),
                "max": float(row["100.0%"])
            }
            print(f"\nTemperature distribution:")
            print(f"  Min: {distribution['min']:.2f}°C")
            print(f"  10th percentile: {distribution['p10']:.2f}°C")
            print(f"  75th percentile: {distribution['p75']:.2f}°C")
            print(f"  90th percentile: {distribution['p90']:.2f}°C")
            print(f"  Max: {distribution['max']:.2f}°C")
            break

# Construct knots for quadratic B-spline (degree 2)
# For degree k=2 with n=5 coefficients, we need n+k+1 = 8 knots
knots = [
    distribution["min"], distribution["min"], distribution["min"],  # Left boundary (3 times)
    distribution["p10"], distribution["p75"], distribution["p90"],  # Internal knots (3 knots)
    distribution["max"], distribution["max"]                         # Right boundary (2 times)
]

print(f"\nKnot vector (length {len(knots)}): {[f'{k:.2f}' for k in knots]}")
print(f"Number of coefficients: {len(coefficients)}")
print(f"Degree: 2")

# Create and evaluate B-spline
bspline = BSpline(knots, coefficients, 2)

# Evaluate at a few test points
test_temps = [distribution["min"], distribution["p10"], distribution["p75"], 
              distribution["p90"], distribution["max"]]
print(f"\nB-spline evaluation at key points:")
for temp in test_temps:
    value = bspline(temp)
    print(f"  T = {temp:6.2f}°C  →  Relative Risk = {value:.6f}")

# Generate full curve
temp_points = np.linspace(distribution["min"], distribution["max"], 100)
y_values = bspline(temp_points)

print(f"\nCurve statistics (100 points):")
print(f"  Min relative risk: {y_values.min():.6f} at {temp_points[y_values.argmin()]:.2f}°C")
print(f"  Max relative risk: {y_values.max():.6f} at {temp_points[y_values.argmax()]:.2f}°C")
print(f"  Mean relative risk: {y_values.mean():.6f}")

print("\n✓ B-spline evaluation successful!")
