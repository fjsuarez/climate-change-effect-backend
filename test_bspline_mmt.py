"""
Test script for B-spline evaluation with MMT centering.
This demonstrates the proper European study methodology.
"""

import csv
import numpy as np
from scipy.interpolate import BSpline

# Load sample data
urau_code = "AT001C"
agegroup = "20-44"

print("="*70)
print("B-SPLINE EXPOSURE-RESPONSE CURVE WITH MMT CENTERING")
print("="*70)

# Load coefficients
with open('data/coefs.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        if row["URAU_CODE"] == urau_code and row["agegroup"] == agegroup:
            coefficients = np.array([float(row[f"b{i}"]) for i in range(1, 6)])
            print(f"\nCity: {urau_code}, Age Group: {agegroup}")
            print(f"Coefficients (b1-b5): {coefficients}")
            break

# Load full temperature distribution
temp_percentiles = {}
with open('data/tmean_distribution.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        if row["URAU_CODE"] == urau_code:
            for col in reader.fieldnames:
                if col != "URAU_CODE" and '%' in col:
                    percentile_val = float(col.replace('%', ''))
                    temp_percentiles[percentile_val] = float(row[col])
            break

# Sort by percentile
sorted_percentiles = sorted(temp_percentiles.items())
percentile_values = np.array([p[0] for p in sorted_percentiles])
temperatures = np.array([p[1] for p in sorted_percentiles])

# Get key percentiles
p10_temp = temp_percentiles[10.0]
p25_temp = temp_percentiles[25.0]
p75_temp = temp_percentiles[75.0]
p90_temp = temp_percentiles[90.0]

print(f"\nTemperature Distribution:")
print(f"  Range: {temperatures.min():.2f}°C to {temperatures.max():.2f}°C")
print(f"  Knots: p10={p10_temp:.2f}°C, p75={p75_temp:.2f}°C, p90={p90_temp:.2f}°C")
print(f"  MMT search range: p25={p25_temp:.2f}°C to p75={p75_temp:.2f}°C")

# Step 1: Construct B-spline basis
knots = np.array([
    temperatures.min(), temperatures.min(), temperatures.min(),
    p10_temp, p75_temp, p90_temp,
    temperatures.max(), temperatures.max()
])

print(f"\nB-spline Specification:")
print(f"  Degree: 2 (quadratic)")
print(f"  Coefficients: {len(coefficients)}")
print(f"  Knots: {len(knots)}")

# Step 2: Evaluate B-spline at all percentiles
bspline = BSpline(knots, coefficients, 2)
log_rr_all = bspline(temperatures)

# Step 3: Identify MMT within 25th-75th percentile range
in_range = (temperatures >= p25_temp) & (temperatures <= p75_temp)
valid_log_rr = log_rr_all[in_range]
valid_temps = temperatures[in_range]

mmt_idx = np.argmin(valid_log_rr)
mmt = valid_temps[mmt_idx]
log_rr_at_mmt = valid_log_rr[mmt_idx]
mmt_percentile = percentile_values[in_range][mmt_idx]

print(f"\nMMT Identification:")
print(f"  MMT = {mmt:.2f}°C ({mmt_percentile:.1f}th percentile)")
print(f"  Log RR at MMT = {log_rr_at_mmt:.6f}")

# Step 4: Center the curve at MMT
log_rr_centered = log_rr_all - log_rr_at_mmt
rr_centered = np.exp(log_rr_centered)

print(f"\nCentering Verification:")
print(f"  RR at MMT = {rr_centered[in_range][mmt_idx]:.10f} ✓ (should be 1.0)")

# Step 5: Extract extreme percentile RRs
rr_at_p01 = rr_centered[percentile_values == 1.0][0]
rr_at_p99 = rr_centered[percentile_values == 99.0][0]

print(f"\nExtreme Relative Risks:")
print(f"  Cold (1st %ile, {temp_percentiles[1.0]:.2f}°C): RR = {rr_at_p01:.4f}")
print(f"  Heat (99th %ile, {temp_percentiles[99.0]:.2f}°C): RR = {rr_at_p99:.4f}")

# Additional statistics
print(f"\nCurve Statistics:")
print(f"  Min RR: {rr_centered.min():.4f} at {temperatures[rr_centered.argmin()]:.2f}°C")
print(f"  Max RR: {rr_centered.max():.4f} at {temperatures[rr_centered.argmax()]:.2f}°C")
print(f"  Mean RR: {rr_centered.mean():.4f}")

# Verify centering
assert np.abs(rr_centered[in_range][mmt_idx] - 1.0) < 1e-10, "RR must be 1.0 at MMT!"

print("\n" + "="*70)
print("✓ B-spline evaluation with MMT centering successful!")
print("="*70)
