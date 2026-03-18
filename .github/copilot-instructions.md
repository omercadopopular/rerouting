# Copilot Instructions: Trade Rerouting Research

## Project Overview
This is an economics research project analyzing trade rerouting effects between Mexico, China, and the US, with welfare implications measured through theoretical economic models. The project combines empirical data analysis using US Census trade data with theoretical welfare calculations.

## Architecture & Data Flow

### 1. Data Collection Pipeline
- **`code/census_trade_download_v2.py`**: Downloads US import data from Census International Trade API
  - Use `--countries Mexico,China` for bilateral trade data by HS-10 codes
  - Use `--hs10 [codes]` for specific product analyses across all partners
  - Always specify `--start` and `--end` periods (YYYY-MM format)
  - Outputs: `chnoutput.csv`, `mexoutput.csv`

- **`code/tariff_hs10_download.py`**: Downloads bilateral tariff data from USITC HTS archives
  - Handles FTA preferences, GSP, and Column 2 tariffs automatically
  - Use `--countries ALL` to get all known trading partners
  - Outputs structured tariff panel: `period × hts10 × country × duty_string × duty_source`

### 2. Data Processing & Analysis
- **`code/merge_mex_china_imports.py`**: Core analysis combining China/Mexico trade data
  - Calculates Mexico's share: `mex_share = mex_val / (china_val + mex_val)`
  - Generates percentile distributions of Mexico shares across HS-10 codes
  - Creates cumulative change analysis from 2018-07 baseline period
  - Outputs: `merged_imports.csv`, `mex_share_percentiles.csv`, time-series plots

### 3. Theoretical Welfare Analysis
- **`welfare-integration.py`**: Implements theoretical welfare model with trade rerouting
  - Key parameters: `sigma=5.0` (substitution), `eta=0.2`, `zeta=0.6` (Pareto shape parameters)
  - Core functions: `S_F_of_lambda()` (foreign goods share), `s_fh_of_lambda()` (direct shipping share)
  - Uses numerical integration (`scipy.quad`) to compute welfare changes: `delta_lnC(lambda0, lambda1)`
  - Generates publication-ready plots in `figs/` directory

## Key Conventions

### File Structure
- `/code/`: All data processing scripts and CSV outputs
- `/figs/`: Publication-ready PDF plots (welfare curves, trade share distributions)
- Root: Main theoretical analysis (`welfare-integration.py`) and final outputs

### Data Handling
- **HS-10 codes**: Always 10-digit strings, zero-padded (`normalize_hts10()`)
- **Period format**: YYYY-MM for monthly data (e.g., "2018-07")
- **Country names**: Use Census canonical names; handle aliases via `COUNTRY_ALIASES` dict
- **Trade values**: `GEN_VAL_MO` from Census API (monthly general import values in USD)

### Plot Styling
- Use `seaborn` whitegrid style with "talk" context
- Standard colors: `navy` for welfare curves, `darkgreen`/`royalblue` for share functions
- Save as PDF to `/figs/` directory with `bbox_inches='tight'`
- Include vertical line at `2018-07-01` to mark trade war onset

## Critical Workflows

### Running Full Analysis
1. Download trade data: `python code/census_trade_download_v2.py --countries Mexico,China --start 2016-01 --end 2024-12`
2. Merge and analyze: `python code/merge_mex_china_imports.py`
3. Generate welfare plots: `python welfare-integration.py`

### Working with APIs
- Census API rate limits: Use `REQUEST_SLEEP = 0.15` between calls
- Batch country queries in groups of 30 (`BATCH_CTRY = 30`)
- Handle HTTP 204 responses (no data) gracefully in country/tariff lookups

### Model Extensions
- When modifying welfare functions, maintain the integration bounds `lambda0=1.0` to `lambda1` grid
- Trade-off parameters (`tau_fh_bar=1.4`, `tauR_bar=1`) represent relative costs of direct vs rerouted trade
- Numerical integration uses `limit=200` for convergence in `quad()`

## Integration Points
- **External APIs**: Census International Trade Time-Series API, USITC HTS archive
- **Dependencies**: pandas, numpy, matplotlib, seaborn, scipy, requests
- **Data Dependencies**: Requires `chnoutput.csv` and `mexoutput.csv` from download scripts before running merge analysis