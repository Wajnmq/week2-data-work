# Week 2 Summary — ETL + EDA

## Key findings
- Revenue by country: SA generated higher total revenue (about 642 SAR from 17 orders) than AE (about 463 SAR from 13 orders).
- Average order value: AE had a higher average order value (about 58 SAR) compared to SA (about 40 SAR).
- Revenue over time: Monthly revenue increased from Aug 2025 to Dec 2025, with a short stabilization around Oct–Nov.

## Definitions
- Revenue = sum of amount for orders with valid numeric values.
- Refund rate = number of refunded orders divided by total orders, where refund is status_clean equal to refund.
- Time window = Aug 2025 to Dec 2025, excluding orders with missing timestamps.

## Data quality caveats
- Missing values exist in amount, quantity, and created_at fields.
- No duplicate order_id values were found.
- All orders were successfully joined with users.
- No statistical outliers were detected.

## Next questions
- Why is the average order value higher in AE than in SA?
- What caused the revenue growth after September?
- How can missing values in the source data be reduced?
