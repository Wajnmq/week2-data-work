## Setup
python -m venv .venv
# activate the virtual environment
pip install -r requirements.txt

## Run ETL
python scripts/run_etl.py

## Outputs
- data/processed/analytics_table.parquet
- data/processed/_run_meta.json
- reports/figures/*.png

## EDA
Open notebooks/eda.ipynb and run all cells.

## Notes
- User and order datasets were created and extended manually to support the ETL and EDA workflow.
