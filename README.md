This project demonstrates a complete data analytics workflow built to transform raw transactional data into clean, analytics-ready outputs and exploratory insights.

What the project does:
- Loads raw CSV data and stores it in Parquet format with enforced schemas.
- Applies data quality checks to ensure required columns exist, datasets are non-empty, and keys are unique.
- Handles missing values by auditing and adding explicit missing-value flags.
- Cleans and normalizes categorical fields to produce consistent values.
- Parses datetime fields and creates time-based features such as year, month, weekday, and hour.
- Combines fact and dimension data using a safe left join to avoid row duplication.
- Detects and handles outliers in monetary values using IQR-based rules and winsorization.
- Produces a final analytics table used for analysis.

Exploratory Data Analysis (EDA) is performed using a Jupyter notebook to:
- Analyze revenue by country.
- Study revenue trends over time.
- Examine transaction amount distributions.
- Compare refund behavior between countries using a bootstrap approach.

The focus of this project is on building a clean, reproducible, and well-validated analytics pipeline rather than predictive modeling.
