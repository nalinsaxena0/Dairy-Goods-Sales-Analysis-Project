/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID ('silver.farms', 'U') IS NOT NULL
    DROP TABLE silver.farms;
GO

CREATE TABLE silver.farms (
    farm_id             INT IDENTITY(1,1) PRIMARY KEY,
    location            NVARCHAR(100),
    total_land_area     FLOAT,             -- value only
    land_area_unit      NVARCHAR(20),      -- e.g., 'acres'
    number_of_cows      INT,
    farm_size           NVARCHAR(50),
    record_date         DATE
);
GO

IF OBJECT_ID ('silver.products', 'U') IS NOT NULL
    DROP TABLE silver.products;
GO

CREATE TABLE silver.products (
    product_key         INT IDENTITY(1,1) PRIMARY KEY,
    product_id          INT,
    product_name        NVARCHAR(100),
    brand               NVARCHAR(100),
    shelf_life          INT,                -- days
    storage_condition   NVARCHAR(100),
    production_date     DATE,
    expiration_date     DATE
);
GO

IF OBJECT_ID ('silver.sales', 'U') IS NOT NULL
    DROP TABLE silver.sales;
GO

CREATE TABLE silver.sales (
    sale_id                 INT IDENTITY(1,1) PRIMARY KEY,
    product_key             INT,

    quantity_value          FLOAT,
    quantity_unit           NVARCHAR(20),
    price_per_unit          FLOAT,

    quantity_sold_value     FLOAT,
    quantity_sold_unit      NVARCHAR(20),

    selling_price           FLOAT,
    revenue                 FLOAT,     -- always INR
    total_value             FLOAT,     -- always INR

    customer_location       NVARCHAR(100),
    sales_channel           NVARCHAR(50),

    FOREIGN KEY (product_key) REFERENCES silver.products(product_key)
);
GO

IF OBJECT_ID ('silver.inventory', 'U') IS NOT NULL
    DROP TABLE silver.inventory;
GO

CREATE TABLE silver.inventory (
    product_key             INT PRIMARY KEY,

    stock_value             FLOAT,
    stock_unit              NVARCHAR(20),

    min_stock_value         FLOAT,
    min_stock_unit          NVARCHAR(20),

    reorder_quantity_value  FLOAT,
    reorder_quantity_unit   NVARCHAR(20),

    FOREIGN KEY (product_key) REFERENCES silver.products(product_key)
);
GO
