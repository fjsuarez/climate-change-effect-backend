"""
Actuarial Climate Risk Engine

A Python-based system for assessing the impact of temperature changes on human mortality
and financial reserves of insurance portfolios.

This module provides:
- MortalityAdjuster: Applies ERF-based temperature shocks to baseline mortality tables
- PortfolioValuation: Calculates insurance reserves using actuarial commutation functions

Author: Climate Risk Analytics Team
"""

import pandas as pd
import numpy as np
from typing import Callable, Optional, Tuple, Dict, Any
from dataclasses import dataclass


# =============================================================================
# MODULE 1: THE MORTALITY SHOCKER
# =============================================================================

class MortalityAdjuster:
    """
    Adjusts baseline mortality tables using Exposure-Response Functions (ERFs)
    to model the impact of temperature changes on mortality.
    
    The mortality adjustment follows the formula:
        adjusted_qx = baseline_qx * RR(temperature_delta)
    
    Where RR is the Relative Risk obtained from the ERF function.
    
    Attributes:
        baseline_table (pd.DataFrame): Original life table with ages and mortality rates
        temperature_delta (float): Temperature change in degrees Celsius
        erf_function (Callable): Function that maps temperature to Relative Risk
    
    Example:
        >>> adjuster = MortalityAdjuster(baseline_df, 2.5, lambda t: 1 + 0.02*t)
        >>> adjusted_table = adjuster.get_comparison_table()
    """
    
    def __init__(
        self,
        baseline_table: pd.DataFrame,
        temperature_delta: float,
        erf_function: Callable[[float], float],
        age_column: str = 'age',
        qx_column: str = 'qx'
    ):
        """
        Initialize the MortalityAdjuster.
        
        Args:
            baseline_table: DataFrame with at minimum age and mortality rate columns
            temperature_delta: Projected temperature change (e.g., +2.5 for 2.5°C warming)
            erf_function: Function f(temp) -> RR that returns relative risk for temperature
            age_column: Name of the age column in baseline_table
            qx_column: Name of the mortality rate column in baseline_table
        """
        self.baseline_table = baseline_table.copy()
        self.temperature_delta = temperature_delta
        self.erf_function = erf_function
        self.age_column = age_column
        self.qx_column = qx_column
        self.radix = 100_000  # Standard actuarial radix
        
        # Validate inputs
        self._validate_inputs()
        
        # Compute the relative risk
        self.relative_risk = self.erf_function(self.temperature_delta)
        
        # Build both life tables
        self._baseline_life_table = self._build_life_table(self.baseline_table[self.qx_column].values)
        self._adjusted_life_table = self._build_adjusted_life_table()
    
    def _validate_inputs(self) -> None:
        """Validate input data integrity."""
        if self.age_column not in self.baseline_table.columns:
            raise ValueError(f"Age column '{self.age_column}' not found in baseline table")
        if self.qx_column not in self.baseline_table.columns:
            raise ValueError(f"Mortality column '{self.qx_column}' not found in baseline table")
        
        qx_values = self.baseline_table[self.qx_column].values
        if np.any(qx_values < 0) or np.any(qx_values > 1):
            raise ValueError("Mortality rates (qx) must be between 0 and 1")
    
    def _build_life_table(self, qx: np.ndarray) -> pd.DataFrame:
        """
        Construct a complete life table from mortality rates.
        
        Life Table Columns:
        - x: Age
        - qx: Probability of death between age x and x+1
        - px: Probability of surviving from age x to x+1 (= 1 - qx)
        - lx: Number of survivors at age x (starting from radix)
        - dx: Number of deaths between age x and x+1 (= lx * qx)
        - Lx: Person-years lived between age x and x+1 (≈ lx - 0.5*dx)
        - Tx: Total person-years lived from age x onwards
        - ex: Life expectancy at age x (= Tx / lx)
        
        Args:
            qx: Array of mortality rates by age
            
        Returns:
            DataFrame with complete life table
        """
        n = len(qx)
        ages = self.baseline_table[self.age_column].values
        
        # Ensure qx is bounded [0, 1]
        qx = np.clip(qx, 0, 1)
        
        # Calculate survival probability
        px = 1 - qx
        
        # Calculate survivors (lx) starting from radix
        lx = np.zeros(n)
        lx[0] = self.radix
        for i in range(1, n):
            lx[i] = lx[i-1] * px[i-1]
        
        # Calculate deaths (dx)
        dx = lx * qx
        
        # Calculate person-years lived (Lx) using linear assumption
        # For ages 0-1, use special infant mortality adjustment
        Lx = np.zeros(n)
        Lx[0] = lx[0] - (1 - 0.3) * dx[0]  # Infant mortality adjustment (a0 ≈ 0.3)
        Lx[1:] = lx[1:] - 0.5 * dx[1:]
        
        # Calculate total person-years from age x onwards (Tx)
        Tx = np.zeros(n)
        Tx[-1] = Lx[-1]
        for i in range(n - 2, -1, -1):
            Tx[i] = Tx[i + 1] + Lx[i]
        
        # Calculate life expectancy (ex)
        ex = np.zeros(n)
        mask = lx > 0
        ex[mask] = Tx[mask] / lx[mask]
        
        return pd.DataFrame({
            'age': ages,
            'qx': qx,
            'px': px,
            'lx': lx,
            'dx': dx,
            'Lx': Lx,
            'Tx': Tx,
            'ex': ex
        })
    
    def _build_adjusted_life_table(self) -> pd.DataFrame:
        """
        Apply the ERF-derived relative risk to create adjusted mortality rates.
        
        The adjustment formula is:
            adjusted_qx = min(1, baseline_qx * RR)
        
        Where RR is the Relative Risk from the ERF function.
        """
        baseline_qx = self.baseline_table[self.qx_column].values
        
        # Apply relative risk adjustment
        adjusted_qx = baseline_qx * self.relative_risk
        
        # Ensure adjusted_qx stays in valid range [0, 1]
        adjusted_qx = np.clip(adjusted_qx, 0, 1)
        
        return self._build_life_table(adjusted_qx)
    
    @property
    def baseline_life_table(self) -> pd.DataFrame:
        """Return the baseline life table."""
        return self._baseline_life_table.copy()
    
    @property
    def adjusted_life_table(self) -> pd.DataFrame:
        """Return the climate-adjusted life table."""
        return self._adjusted_life_table.copy()
    
    def get_comparison_table(self) -> pd.DataFrame:
        """
        Generate a comparison DataFrame showing baseline vs adjusted values.
        
        Returns:
            DataFrame with columns for both scenarios and their differences
        """
        comparison = pd.DataFrame({
            'age': self._baseline_life_table['age'],
            'baseline_qx': self._baseline_life_table['qx'],
            'adjusted_qx': self._adjusted_life_table['qx'],
            'baseline_lx': self._baseline_life_table['lx'],
            'adjusted_lx': self._adjusted_life_table['lx'],
            'baseline_ex': self._baseline_life_table['ex'],
            'adjusted_ex': self._adjusted_life_table['ex'],
        })
        
        # Calculate deltas
        comparison['delta_qx'] = comparison['adjusted_qx'] - comparison['baseline_qx']
        comparison['delta_qx_pct'] = (comparison['delta_qx'] / comparison['baseline_qx']) * 100
        comparison['delta_lx'] = comparison['adjusted_lx'] - comparison['baseline_lx']
        comparison['delta_ex'] = comparison['adjusted_ex'] - comparison['baseline_ex']
        
        return comparison
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """
        Calculate summary statistics for the mortality adjustment.
        
        Returns:
            Dictionary containing key metrics and comparisons
        """
        baseline = self._baseline_life_table
        adjusted = self._adjusted_life_table
        
        return {
            'temperature_delta': self.temperature_delta,
            'relative_risk': self.relative_risk,
            'baseline_life_expectancy_at_birth': baseline.loc[0, 'ex'],
            'adjusted_life_expectancy_at_birth': adjusted.loc[0, 'ex'],
            'life_expectancy_change': adjusted.loc[0, 'ex'] - baseline.loc[0, 'ex'],
            'avg_mortality_increase_pct': ((adjusted['qx'].mean() / baseline['qx'].mean()) - 1) * 100,
            'radix': self.radix
        }


# =============================================================================
# MODULE 2: THE RESERVE CALCULATOR
# =============================================================================

@dataclass
class CommutationFunctions:
    """
    Actuarial Commutation Functions for life contingencies calculations.
    
    Commutation functions are auxiliary values used to simplify the calculation
    of actuarial present values. They are computed from the life table and
    a discount rate.
    
    Functions:
        Dx = v^x * lx           (discounted survivors)
        Nx = Σ(Dx to ω)         (sum of Dx from age x to end)
        Cx = v^(x+1) * dx       (discounted deaths)
        Mx = Σ(Cx to ω)         (sum of Cx from age x to end)
    
    Where v = 1/(1+i) is the discount factor and i is the interest rate.
    """
    age: np.ndarray
    Dx: np.ndarray
    Nx: np.ndarray
    Cx: np.ndarray
    Mx: np.ndarray
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert commutation functions to DataFrame."""
        return pd.DataFrame({
            'age': self.age,
            'Dx': self.Dx,
            'Nx': self.Nx,
            'Cx': self.Cx,
            'Mx': self.Mx
        })


class PortfolioValuation:
    """
    Calculates insurance reserves for a portfolio using actuarial commutation functions.
    
    This class implements standard actuarial valuation formulas:
    
    Whole Life Annuity Due (äx):
        äx = Nx / Dx
        Present value of $1 per year payable at the beginning of each year
        while the annuitant survives.
    
    Whole Life Insurance (Ax):
        Ax = Mx / Dx
        Present value of $1 payable at the end of the year of death.
    
    The reserve for each policy is calculated as:
        - Annuity: volume * äx (company pays while policyholder lives)
        - Life Insurance: volume * Ax (company pays when policyholder dies)
    
    Attributes:
        life_table (pd.DataFrame): Life table with mortality rates
        interest_rate (float): Technical interest rate (annual)
        portfolio_data (pd.DataFrame): Policyholder data with age, product, volume
    """
    
    def __init__(
        self,
        life_table: pd.DataFrame,
        interest_rate: float,
        portfolio_data: pd.DataFrame
    ):
        """
        Initialize the PortfolioValuation calculator.
        
        Args:
            life_table: DataFrame with 'age', 'lx', 'dx' columns
            interest_rate: Annual technical interest rate (e.g., 0.01 for 1%)
            portfolio_data: DataFrame with 'age', 'product_type', 'volume' columns
        """
        self.life_table = life_table.copy()
        self.interest_rate = interest_rate
        self.portfolio_data = portfolio_data.copy()
        self.discount_factor = 1 / (1 + interest_rate)  # v = 1/(1+i)
        
        # Validate inputs
        self._validate_inputs()
        
        # Build commutation functions
        self._commutation = self._build_commutation_functions()
        
        # Calculate actuarial values
        self._actuarial_values = self._calculate_actuarial_values()
    
    def _validate_inputs(self) -> None:
        """Validate input data."""
        required_life_cols = ['age', 'lx', 'dx']
        for col in required_life_cols:
            if col not in self.life_table.columns:
                raise ValueError(f"Life table missing required column: {col}")
        
        required_portfolio_cols = ['age', 'product_type', 'volume']
        for col in required_portfolio_cols:
            if col not in self.portfolio_data.columns:
                raise ValueError(f"Portfolio data missing required column: {col}")
        
        valid_products = {'Annuity', 'Life Insurance'}
        invalid = set(self.portfolio_data['product_type'].unique()) - valid_products
        if invalid:
            raise ValueError(f"Invalid product types: {invalid}. Must be 'Annuity' or 'Life Insurance'")
        
        if self.interest_rate < 0:
            raise ValueError("Interest rate must be non-negative")
    
    def _build_commutation_functions(self) -> CommutationFunctions:
        """
        Compute commutation functions from the life table.
        
        Formulas:
            Dx = v^x * lx       (discounted number of survivors)
            Cx = v^(x+1) * dx   (discounted number of deaths)
            Nx = Σ Dt for t ≥ x (sum of Dx from age x to omega)
            Mx = Σ Ct for t ≥ x (sum of Cx from age x to omega)
        """
        ages = self.life_table['age'].values
        lx = self.life_table['lx'].values
        dx = self.life_table['dx'].values
        v = self.discount_factor
        
        n = len(ages)
        
        # Dx = v^x * lx
        Dx = np.array([v**x * lx[i] for i, x in enumerate(ages)])
        
        # Cx = v^(x+1) * dx
        Cx = np.array([v**(x+1) * dx[i] for i, x in enumerate(ages)])
        
        # Nx = sum of Dx from age x to end (cumulative sum from end)
        Nx = np.zeros(n)
        Nx[-1] = Dx[-1]
        for i in range(n - 2, -1, -1):
            Nx[i] = Nx[i + 1] + Dx[i]
        
        # Mx = sum of Cx from age x to end (cumulative sum from end)
        Mx = np.zeros(n)
        Mx[-1] = Cx[-1]
        for i in range(n - 2, -1, -1):
            Mx[i] = Mx[i + 1] + Cx[i]
        
        return CommutationFunctions(
            age=ages,
            Dx=Dx,
            Nx=Nx,
            Cx=Cx,
            Mx=Mx
        )
    
    def _calculate_actuarial_values(self) -> pd.DataFrame:
        """
        Calculate actuarial present values for each age.
        
        Values computed:
            äx (a_double_dot_x): Whole life annuity due = Nx/Dx
            Ax: Whole life insurance = Mx/Dx
        """
        comm = self._commutation
        
        # Avoid division by zero for ages where Dx = 0
        Dx_safe = np.where(comm.Dx > 0, comm.Dx, np.inf)
        
        # äx = Nx / Dx (Whole Life Annuity Due)
        a_x = comm.Nx / Dx_safe
        
        # Ax = Mx / Dx (Whole Life Insurance)
        A_x = comm.Mx / Dx_safe
        
        return pd.DataFrame({
            'age': comm.age,
            'Dx': comm.Dx,
            'Nx': comm.Nx,
            'Mx': comm.Mx,
            'annuity_due_ax': a_x,
            'whole_life_Ax': A_x
        })
    
    @property
    def commutation_functions(self) -> pd.DataFrame:
        """Return commutation functions as DataFrame."""
        return self._commutation.to_dataframe()
    
    @property
    def actuarial_values(self) -> pd.DataFrame:
        """Return actuarial present values by age."""
        return self._actuarial_values.copy()
    
    def calculate_policy_reserves(self) -> pd.DataFrame:
        """
        Calculate reserves for each policy in the portfolio.
        
        Reserve formulas:
            - Annuity: Reserve = Volume × äx
            - Life Insurance: Reserve = Volume × Ax
        
        Returns:
            DataFrame with policy details and calculated reserves
        """
        # Merge portfolio with actuarial values
        result = self.portfolio_data.merge(
            self._actuarial_values[['age', 'annuity_due_ax', 'whole_life_Ax']],
            on='age',
            how='left'
        )
        
        # Calculate reserves based on product type
        def calc_reserve(row):
            if row['product_type'] == 'Annuity':
                return row['volume'] * row['annuity_due_ax']
            else:  # Life Insurance
                return row['volume'] * row['whole_life_Ax']
        
        result['reserve'] = result.apply(calc_reserve, axis=1)
        
        return result
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """
        Generate summary statistics for the portfolio valuation.
        
        Returns:
            Dictionary with total reserves by product type and overall
        """
        reserves = self.calculate_policy_reserves()
        
        annuity_reserves = reserves[reserves['product_type'] == 'Annuity']['reserve'].sum()
        insurance_reserves = reserves[reserves['product_type'] == 'Life Insurance']['reserve'].sum()
        total_reserves = reserves['reserve'].sum()
        
        return {
            'interest_rate': self.interest_rate,
            'total_policies': len(reserves),
            'annuity_policies': len(reserves[reserves['product_type'] == 'Annuity']),
            'life_insurance_policies': len(reserves[reserves['product_type'] == 'Life Insurance']),
            'total_annuity_volume': reserves[reserves['product_type'] == 'Annuity']['volume'].sum(),
            'total_insurance_volume': reserves[reserves['product_type'] == 'Life Insurance']['volume'].sum(),
            'annuity_reserves': annuity_reserves,
            'life_insurance_reserves': insurance_reserves,
            'total_reserves': total_reserves
        }


# =============================================================================
# CLIMATE RISK ENGINE - COMBINED ANALYSIS
# =============================================================================

class ClimateRiskEngine:
    """
    Complete Climate Risk Analysis Engine combining mortality adjustment
    and portfolio valuation.
    
    This class orchestrates the full analysis pipeline:
    1. Apply temperature shock to baseline mortality using ERF
    2. Calculate reserves under both baseline and adjusted scenarios
    3. Quantify the financial impact (delta) of climate change
    
    Example:
        >>> engine = ClimateRiskEngine(
        ...     baseline_mortality=mortality_df,
        ...     portfolio=portfolio_df,
        ...     interest_rate=0.01,
        ...     temperature_delta=2.5,
        ...     erf_function=lambda t: 1 + 0.02*t
        ... )
        >>> report = engine.generate_impact_report()
    """
    
    def __init__(
        self,
        baseline_mortality: pd.DataFrame,
        portfolio: pd.DataFrame,
        interest_rate: float,
        temperature_delta: float,
        erf_function: Callable[[float], float],
        age_column: str = 'age',
        qx_column: str = 'qx'
    ):
        """
        Initialize the Climate Risk Engine.
        
        Args:
            baseline_mortality: Baseline life table with ages and mortality rates
            portfolio: Portfolio data with age, product_type, volume
            interest_rate: Technical interest rate for discounting
            temperature_delta: Temperature change scenario (°C)
            erf_function: Exposure-Response Function mapping temp to RR
            age_column: Column name for age in mortality table
            qx_column: Column name for mortality rate in mortality table
        """
        self.temperature_delta = temperature_delta
        self.interest_rate = interest_rate
        
        # Step 1: Create adjusted mortality table
        self.mortality_adjuster = MortalityAdjuster(
            baseline_table=baseline_mortality,
            temperature_delta=temperature_delta,
            erf_function=erf_function,
            age_column=age_column,
            qx_column=qx_column
        )
        
        # Step 2: Calculate reserves under baseline scenario
        self.baseline_valuation = PortfolioValuation(
            life_table=self.mortality_adjuster.baseline_life_table,
            interest_rate=interest_rate,
            portfolio_data=portfolio
        )
        
        # Step 3: Calculate reserves under adjusted scenario
        self.adjusted_valuation = PortfolioValuation(
            life_table=self.mortality_adjuster.adjusted_life_table,
            interest_rate=interest_rate,
            portfolio_data=portfolio
        )
    
    def generate_impact_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive climate impact report.
        
        Returns:
            Dictionary containing:
            - Mortality impact summary
            - Baseline reserves
            - Adjusted reserves
            - Financial delta (climate impact)
        """
        mortality_summary = self.mortality_adjuster.get_summary_statistics()
        baseline_summary = self.baseline_valuation.get_portfolio_summary()
        adjusted_summary = self.adjusted_valuation.get_portfolio_summary()
        
        # Calculate deltas
        delta_annuity = adjusted_summary['annuity_reserves'] - baseline_summary['annuity_reserves']
        delta_insurance = adjusted_summary['life_insurance_reserves'] - baseline_summary['life_insurance_reserves']
        delta_total = adjusted_summary['total_reserves'] - baseline_summary['total_reserves']
        
        return {
            'scenario': {
                'temperature_delta_celsius': self.temperature_delta,
                'relative_risk': mortality_summary['relative_risk'],
                'interest_rate': self.interest_rate
            },
            'mortality_impact': {
                'baseline_life_expectancy': mortality_summary['baseline_life_expectancy_at_birth'],
                'adjusted_life_expectancy': mortality_summary['adjusted_life_expectancy_at_birth'],
                'life_expectancy_change_years': mortality_summary['life_expectancy_change'],
                'average_mortality_increase_pct': mortality_summary['avg_mortality_increase_pct']
            },
            'baseline_reserves': {
                'annuities': baseline_summary['annuity_reserves'],
                'life_insurance': baseline_summary['life_insurance_reserves'],
                'total': baseline_summary['total_reserves']
            },
            'adjusted_reserves': {
                'annuities': adjusted_summary['annuity_reserves'],
                'life_insurance': adjusted_summary['life_insurance_reserves'],
                'total': adjusted_summary['total_reserves']
            },
            'climate_impact_delta': {
                'annuities': delta_annuity,
                'annuities_interpretation': 'Negative = lower reserves needed (shorter life expectancy)' if delta_annuity < 0 else 'Positive = higher reserves needed',
                'life_insurance': delta_insurance,
                'life_insurance_interpretation': 'Negative = lower reserves needed (deaths occur sooner, less discounting)' if delta_insurance < 0 else 'Positive = higher reserves needed',
                'total': delta_total,
                'total_pct_change': (delta_total / baseline_summary['total_reserves']) * 100 if baseline_summary['total_reserves'] > 0 else 0
            },
            'portfolio_stats': {
                'total_policies': baseline_summary['total_policies'],
                'annuity_policies': baseline_summary['annuity_policies'],
                'life_insurance_policies': baseline_summary['life_insurance_policies'],
                'total_annuity_volume': baseline_summary['total_annuity_volume'],
                'total_insurance_volume': baseline_summary['total_insurance_volume']
            }
        }
    
    def get_detailed_comparison(self) -> pd.DataFrame:
        """
        Get detailed policy-by-policy comparison of reserves.
        
        Returns:
            DataFrame with baseline and adjusted reserves for each policy
        """
        baseline_reserves = self.baseline_valuation.calculate_policy_reserves()
        adjusted_reserves = self.adjusted_valuation.calculate_policy_reserves()
        
        comparison = baseline_reserves[['age', 'product_type', 'volume']].copy()
        comparison['baseline_reserve'] = baseline_reserves['reserve']
        comparison['adjusted_reserve'] = adjusted_reserves['reserve']
        comparison['delta'] = comparison['adjusted_reserve'] - comparison['baseline_reserve']
        comparison['delta_pct'] = (comparison['delta'] / comparison['baseline_reserve']) * 100
        
        return comparison


# =============================================================================
# SAMPLE DATA GENERATORS
# =============================================================================

def generate_sample_mortality_table(max_age: int = 100) -> pd.DataFrame:
    """
    Generate a sample mortality table using Gompertz-Makeham formula.
    
    The Gompertz-Makeham law of mortality:
        μ(x) = A + B * c^x
    
    Where:
        A = age-independent mortality (accidents, etc.)
        B = initial mortality level
        c = rate of mortality increase
    
    We convert force of mortality to qx using:
        qx ≈ 1 - exp(-μ(x))
    
    Args:
        max_age: Maximum age in the table (default 100)
    
    Returns:
        DataFrame with 'age' and 'qx' columns
    """
    ages = np.arange(0, max_age + 1)
    
    # Gompertz-Makeham parameters (approximate US mortality)
    A = 0.0001  # Background mortality
    B = 0.00003  # Gompertz parameter
    c = 1.10  # Rate of increase
    
    # Calculate force of mortality
    mu = A + B * np.power(c, ages)
    
    # Convert to probability of death
    qx = 1 - np.exp(-mu)
    
    # Adjust infant mortality (higher in first year)
    qx[0] = 0.006  # Infant mortality rate
    
    # Cap maximum mortality at 1
    qx = np.clip(qx, 0, 1)
    
    # Force last age to have qx = 1 (certain death)
    qx[-1] = 1.0
    
    return pd.DataFrame({
        'age': ages,
        'qx': qx
    })


def generate_sample_portfolio(
    n_policies: int = 100,
    seed: int = 42
) -> pd.DataFrame:
    """
    Generate a sample insurance portfolio.
    
    Args:
        n_policies: Number of policies to generate
        seed: Random seed for reproducibility
    
    Returns:
        DataFrame with 'age', 'product_type', 'volume' columns
    """
    np.random.seed(seed)
    
    # Generate random ages (weighted towards middle ages)
    ages = np.random.choice(
        range(25, 80),
        size=n_policies,
        p=np.array([1/(1 + abs(x-50)/20) for x in range(25, 80)]) / 
          sum([1/(1 + abs(x-50)/20) for x in range(25, 80)])
    )
    
    # Randomly assign product types (60% annuities, 40% life insurance)
    product_types = np.random.choice(
        ['Annuity', 'Life Insurance'],
        size=n_policies,
        p=[0.6, 0.4]
    )
    
    # Generate volumes based on product type
    volumes = []
    for product in product_types:
        if product == 'Annuity':
            # Annuities: $50k - $500k
            volumes.append(np.random.uniform(50_000, 500_000))
        else:
            # Life Insurance: $100k - $1M
            volumes.append(np.random.uniform(100_000, 1_000_000))
    
    return pd.DataFrame({
        'age': ages,
        'product_type': product_types,
        'volume': volumes
    })


def create_erf_function(
    base_risk: float = 1.0,
    risk_per_degree: float = 0.02,
    nonlinear: bool = False
) -> Callable[[float], float]:
    """
    Create an Exposure-Response Function for temperature-mortality relationship.
    
    Linear model:
        RR(ΔT) = base_risk + risk_per_degree * ΔT
    
    Nonlinear (exponential) model:
        RR(ΔT) = base_risk * exp(risk_per_degree * ΔT)
    
    Args:
        base_risk: Baseline relative risk (usually 1.0)
        risk_per_degree: Additional risk per degree Celsius
        nonlinear: If True, use exponential model
    
    Returns:
        Function that maps temperature delta to relative risk
    """
    if nonlinear:
        return lambda t: base_risk * np.exp(risk_per_degree * t)
    else:
        return lambda t: base_risk + risk_per_degree * t


# =============================================================================
# DEMONSTRATION FUNCTION
# =============================================================================

def run_demonstration():
    """
    Run a complete demonstration of the Climate Risk Engine.
    
    This function:
    1. Generates sample mortality and portfolio data
    2. Runs analysis for multiple temperature scenarios
    3. Prints comprehensive results
    """
    print("=" * 70)
    print("ACTUARIAL CLIMATE RISK ENGINE - DEMONSTRATION")
    print("=" * 70)
    
    # Generate sample data
    print("\n1. GENERATING SAMPLE DATA")
    print("-" * 40)
    
    mortality_table = generate_sample_mortality_table(max_age=100)
    print(f"   Mortality table: {len(mortality_table)} ages (0-100)")
    
    portfolio = generate_sample_portfolio(n_policies=100)
    print(f"   Portfolio: {len(portfolio)} policies")
    print(f"   - Annuities: {len(portfolio[portfolio['product_type'] == 'Annuity'])}")
    print(f"   - Life Insurance: {len(portfolio[portfolio['product_type'] == 'Life Insurance'])}")
    
    # Define parameters
    interest_rate = 0.01  # 1%
    temperature_scenarios = [1.5, 2.0, 2.5, 3.0]  # °C increases
    
    # Create ERF function (2% mortality increase per degree)
    erf = create_erf_function(base_risk=1.0, risk_per_degree=0.02)
    
    print(f"\n2. ANALYSIS PARAMETERS")
    print("-" * 40)
    print(f"   Interest rate: {interest_rate*100:.1f}%")
    print(f"   ERF: Linear, +2% mortality per °C")
    print(f"   Temperature scenarios: {temperature_scenarios} °C")
    
    # Run analysis for each scenario
    print(f"\n3. CLIMATE IMPACT ANALYSIS")
    print("-" * 40)
    
    results = []
    for temp_delta in temperature_scenarios:
        engine = ClimateRiskEngine(
            baseline_mortality=mortality_table,
            portfolio=portfolio,
            interest_rate=interest_rate,
            temperature_delta=temp_delta,
            erf_function=erf
        )
        report = engine.generate_impact_report()
        results.append(report)
        
        print(f"\n   Scenario: +{temp_delta}°C")
        print(f"   Relative Risk: {report['scenario']['relative_risk']:.4f}")
        print(f"   Life Expectancy Change: {report['mortality_impact']['life_expectancy_change_years']:.2f} years")
        print(f"   Baseline Reserves: ${report['baseline_reserves']['total']:,.0f}")
        print(f"   Adjusted Reserves: ${report['adjusted_reserves']['total']:,.0f}")
        print(f"   Climate Impact (Δ): ${report['climate_impact_delta']['total']:,.0f} "
              f"({report['climate_impact_delta']['total_pct_change']:.2f}%)")
    
    # Print detailed breakdown for 2.5°C scenario
    print(f"\n4. DETAILED BREAKDOWN (+2.5°C SCENARIO)")
    print("-" * 40)
    
    engine_25 = ClimateRiskEngine(
        baseline_mortality=mortality_table,
        portfolio=portfolio,
        interest_rate=interest_rate,
        temperature_delta=2.5,
        erf_function=erf
    )
    
    report = engine_25.generate_impact_report()
    
    print(f"\n   MORTALITY IMPACT:")
    print(f"   - Baseline Life Expectancy: {report['mortality_impact']['baseline_life_expectancy']:.2f} years")
    print(f"   - Adjusted Life Expectancy: {report['mortality_impact']['adjusted_life_expectancy']:.2f} years")
    print(f"   - Change: {report['mortality_impact']['life_expectancy_change_years']:.2f} years")
    print(f"   - Avg Mortality Increase: {report['mortality_impact']['average_mortality_increase_pct']:.2f}%")
    
    print(f"\n   RESERVE IMPACT BY PRODUCT:")
    print(f"   Annuities:")
    print(f"   - Baseline: ${report['baseline_reserves']['annuities']:,.0f}")
    print(f"   - Adjusted: ${report['adjusted_reserves']['annuities']:,.0f}")
    print(f"   - Delta: ${report['climate_impact_delta']['annuities']:,.0f}")
    
    print(f"\n   Life Insurance:")
    print(f"   - Baseline: ${report['baseline_reserves']['life_insurance']:,.0f}")
    print(f"   - Adjusted: ${report['adjusted_reserves']['life_insurance']:,.0f}")
    print(f"   - Delta: ${report['climate_impact_delta']['life_insurance']:,.0f}")
    
    print("\n" + "=" * 70)
    print("DEMONSTRATION COMPLETE")
    print("=" * 70)
    
    return results


if __name__ == "__main__":
    run_demonstration()
