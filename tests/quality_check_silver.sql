/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- Start of silver.farms quality checks
SELECT DISTINCT location
FROM bronze.dairy_info;

SELECT location
FROM bronze.dairy_info
WHERE location IS NULL;

SELECT location
FROM bronze.dairy_info
WHERE location <> TRIM(location);

SELECT total_land_area
FROM bronze.dairy_info
WHERE total_land_area IS NULL;

SELECT number_of_cows
FROM bronze.dairy_info
WHERE number_of_cows IS NULL;

SELECT DISTINCT farm_size
FROM bronze.dairy_info;

SELECT farm_size
FROM bronze.dairy_info
WHERE farm_size IS NULL;
-- End of silver.farms quality checks 

-- Start of silver.products quality checks
SELECT product_id
FROM bronze.dairy_info
WHERE product_id IS NULL;

select product_name
FROM bronze.dairy_info
WHERE product_name IS NULL;

select product_name
FROM bronze.dairy_info
WHERE product_name <> TRIM(product_name);

SELECT production_date, expiration_date
FROM bronze.dairy_info
WHERE production_date > expiration_date;
-- End of silver.products quality checks 

-- Start of silver.sales quality checks
SELECT quantity
FROM bronze.dairy_info
WHERE quantity IS NULL;

SELECT DISTINCT product_name
FROM bronze.dairy_info;
-- End of silver.sales quality checks 
