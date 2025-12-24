# week2-data-work

A small data workflow project that:
- loads raw CSVs (orders, users)
- cleans and validates data
- builds an analytics-ready table with time features, safe joins, and outlier handling

## Structure
- `src/bootcamp_data/` reusable code (paths, IO, transforms, joins, quality)
- `scripts/` runnable pipelines
- `data/raw/` input CSVs
- `data/processed/` generated Parquet outputs
- `reports/` quality reports

## How to run
Install:
```bash
pip install -r requirements.txt
