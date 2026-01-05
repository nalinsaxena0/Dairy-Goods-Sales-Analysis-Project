/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.dairy_info', 'U') IS NOT NULL
	DROP TABLE bronze.dairy_info;
GO

CREATE TABLE bronze.dairy_info (
Location			NVARCHAR(50),
total_land_area		FLOAT,
number_of_cows		INT,
farm_size			NVARCHAR(50),
date				DATE,
product_id			INT,
product_name		NVARCHAR(50),
brand				NVARCHAR(50),
quantity			FLOAT,
price_per_unit		FLOAT,
total_value			FLOAT,
shelf_life			INT,
storage_condition	NVARCHAR(50),
production_date		DATE,
Expiration_date		DATE,
quantity_sold		INT,
selling_price		FLOAT,
revenue				FLOAT,
customer_location	NVARCHAR(50),
sales_channel		NVARCHAR(50),
stk_quantity		INT,
min_stk_threshold	FLOAT,
reorder_quantity	FLOAT
);
GO


