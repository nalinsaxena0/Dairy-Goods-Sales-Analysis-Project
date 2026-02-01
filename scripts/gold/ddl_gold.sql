/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- Creating VIEW gold.dim_products

IF OBJECT_ID ('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
product_key,
product_name,
brand,
shelf_life,
storage_condition
FROM silver.products;
GO

-- Creating VIEW gold.dim_customer_location

IF OBJECT_ID ('gold.dim_customer_location', 'V') IS NOT NULL
	DROP VIEW gold.dim_customer_location;
GO

CREATE VIEW gold.dim_customer_location AS
SELECT DISTINCT
	DENSE_RANK () OVER (ORDER BY customer_location) AS location_key,
	customer_location
FROM silver.sales;
GO

-- Creating VIEW gold.dim_sales_channel

IF OBJECT_ID ('gold.dim_sales_channel', 'V') IS NOT NULL
	DROP VIEW gold.dim_sales_channel;
GO

CREATE VIEW gold.dim_sales_channel AS
SELECT DISTINCT
DENSE_RANK() OVER (ORDER BY sales_channel) AS sales_channel_key,
sales_channel
FROM silver.sales;
GO

-- Creating VIEW gold.dim_dates

IF OBJECT_ID('gold.dim_dates', 'V') IS NOT NULL
    DROP VIEW gold.dim_dates;
GO

CREATE VIEW gold.dim_dates AS
WITH DateRange AS (
    SELECT  
        MIN(production_date) AS start_date,
        MAX(production_date) AS end_date
    FROM silver.products
),
Tally AS (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM master..spt_values  
)

SELECT
    CONVERT(INT, FORMAT(DATEADD(DAY, n, start_date), 'yyyyMMdd')) AS date_key,
    DATEADD(DAY, n, start_date) AS full_date,

    YEAR(DATEADD(DAY, n, start_date)) AS year,
    MONTH(DATEADD(DAY, n, start_date)) AS month_number,
    DATENAME(MONTH, DATEADD(DAY, n, start_date)) AS month_name,

    DATEPART(QUARTER, DATEADD(DAY, n, start_date)) AS quarter_number,
    DAY(DATEADD(DAY, n, start_date)) AS day_number,
    DATENAME(WEEKDAY, DATEADD(DAY, n, start_date)) AS day_name,

    DATEPART(WEEK, DATEADD(DAY, n, start_date)) AS week_of_year,
    CASE 
        WHEN DATENAME(WEEKDAY, DATEADD(DAY, n, start_date)) IN ('Saturday', 'Sunday')
            THEN 1 ELSE 0 
    END AS is_weekend

FROM DateRange
CROSS JOIN Tally
WHERE DATEADD(DAY, n, start_date) <= end_date;
GO

-- Creating VIEW gold.fact_sales_daily

IF OBJECT_ID ('gold.fact_sales_daily', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales_daily;
GO

CREATE VIEW gold.fact_sales_daily AS
SELECT
d.date_key,
p.product_key,
l.location_key,
sc.sales_channel_key,
SUM(quantity_sold_value) AS total_quantity_sold,
ROUND(SUM(revenue), 2) AS total_revenue,
ROUND(SUM(total_value), 2) AS total_value,
ROUND(AVG(selling_price), 2) AS avg_selling_price,
ROUND(AVG(price_per_unit), 2) AS avg_price_per_unit,
ROUND(SUM(revenue) * 1.0 / NULLIF(SUM(quantity_sold_value), 0), 2) AS revenue_per_unit,
CASE
    WHEN SUM(revenue) < 1000 THEN 'Low'
    WHEN SUM(revenue) < 5000 THEN 'Medium'
    ELSE 'High'
END AS revenue_category,

CASE
    WHEN SUM(quantity_sold_value) < 50 THEN 'Low Volume'
    WHEN SUM(quantity_sold_value) < 200 THEN 'Medium Volume'
    ELSE 'High Volume'
END AS quantity_category
FROM silver.sales s
INNER JOIN silver.products p
ON s.product_key = p.product_key
INNER JOIN gold.dim_dates d
ON p.production_date = d.full_date
INNER JOIN gold.dim_customer_location l
ON s.customer_location = l.customer_location
INNER JOIN gold.dim_sales_channel sc
ON s.sales_channel = sc.sales_channel
GROUP BY d.date_key, p.product_key, l.location_key, sc.sales_channel_key;
GO

-- Creating VIEW gold.fact_inventory_snapshot

IF OBJECT_ID('gold.fact_inventory_snapshot', 'V') IS NOT NULL
    DROP VIEW gold.fact_inventory_snapshot;
GO

CREATE VIEW gold.fact_inventory_snapshot AS
SELECT
d.date_key,
i.product_key,
i.stock_value,
i.min_stock_value,
i.reorder_quantity_value,
CASE 
     WHEN i.stock_value <= i.min_stock_value THEN 1 
     ELSE 0 
END AS needs_reorder,
CASE
    WHEN i.stock_value = 0 THEN 'Out of Stock'
    WHEN i.stock_value < i.min_stock_value THEN 'Low Stock'
    ELSE 'Healthy Stock'
END AS stock_status,

(i.min_stock_value - i.stock_value) AS reorder_gap
FROM silver.inventory i
CROSS JOIN (
	SELECT date_key, full_date
	FROM gold.dim_dates
	WHERE full_date = (SELECT MAX(full_date) FROM gold.dim_dates)
) d;
GO
