/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==============================';
        PRINT 'Loading Silver Layer';
        PRINT '==============================';

        -- Clearing Fact Tables first:
        PRINT '>> Truncating Table: silver.sales';
        TRUNCATE TABLE silver.sales;
        
        PRINT '>> Truncating Table: silver.inventory';
        TRUNCATE TABLE silver.inventory;
        
        -- Clearing Dimension Table Now:
        PRINT '>> Deleting Rows From Table: silver.products';
        DELETE FROM silver.products;
        DBCC CHECKIDENT ('silver.products', RESEED, 0);


        -- Loading silver.farms
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: slver.farms';
        TRUNCATE TABLE silver.farms;
        PRINT '>> Inserting Data Into: silver.farms'
        INSERT INTO silver.farms (
        location,
        total_land_area,
        land_area_unit,
        number_of_cows,
        farm_size,
        record_date
        )

        SELECT 
        TRIM(location) AS location,
        ISNULL(total_land_area, 0) AS total_land_area,
        'Acres' AS land_area_unit,
        ISNULL(number_of_cows, 0) AS number_of_cows,
        farm_size,
        date AS record_date
        FROM bronze.dairy_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        --Loading silver.products
        SET @start_time = GETDATE();        
        PRINT '>> Inserting Data Into: silver.farms';
        WITH ranked AS (
            SELECT
                b.product_id AS product_id,
                TRIM(b.product_name) AS product_name,
                TRIM(ISNULL(b.brand, 'n/a')) AS brand,
                b.shelf_life,
                TRIM(ISNULL(b.storage_condition, 'n/a')) AS storage_condition,
                b.production_date,
                b.expiration_date,
                ROW_NUMBER() OVER (
                    PARTITION BY b.product_name, b.brand
                    ORDER BY b.production_date DESC
                ) AS rn
            FROM bronze.dairy_info b
        )
        
        INSERT INTO silver.products (
            product_id,
            product_name,
            brand,
            shelf_life,
            storage_condition,
            production_date,
            expiration_date
        )
        SELECT
            product_id,
            product_name,
            brand,
            shelf_life,
            storage_condition,
            production_date,
            expiration_date
        FROM ranked
        WHERE rn = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        --Loading silver.sales
        SET @start_time = GETDATE();   
        PRINT '>> Inserting Data Into: silver.sales'
        INSERT INTO silver.sales (
        product_key,
        quantity_value,
        quantity_unit,
        price_per_unit,
        quantity_sold_value,
        quantity_sold_unit,
        selling_price,
        revenue,
        total_value,
        customer_location,
        sales_channel
        )

        SELECT
        p.product_key,
        ISNULL(quantity, 0) AS quantity_value,
        CASE
	        WHEN b.product_name = 'Ice Cream' THEN 'Kg'
	        WHEN b.product_name = 'Buttermilk' THEN 'Litres'
	        WHEN b.product_name = 'Cheese' THEN 'Kg'
	        WHEN b.product_name = 'Butter' THEN 'Kg'
	        WHEN b.product_name = 'Lassi' THEN 'Litres'
	        WHEN b.product_name = 'Yogurt' THEN 'Litres'
	        WHEN b.product_name = 'Curd' THEN 'Litres'
	        WHEN b.product_name = 'Milk' THEN 'Litres'
	        WHEN b.product_name = 'Paneer' THEN 'Kg'
	        WHEN b.product_name = 'Ghee' THEN 'Litres'
        END AS quantity_unit,
        ISNULL(price_per_unit, 0) AS price_per_unit,
        ISNULL(quantity_sold, 0) AS quantity_sold_value,
        CASE
	        WHEN b.product_name = 'Ice Cream' THEN 'Kg'
	        WHEN b.product_name = 'Buttermilk' THEN 'Litres'
	        WHEN b.product_name = 'Cheese' THEN 'Kg'
	        WHEN b.product_name = 'Butter' THEN 'Kg'
	        WHEN b.product_name = 'Lassi' THEN 'Litres'
	        WHEN b.product_name = 'Yogurt' THEN 'Litres'
	        WHEN b.product_name = 'Curd' THEN 'Litres'
	        WHEN b.product_name = 'Milk' THEN 'Litres'
	        WHEN b.product_name = 'Paneer' THEN 'Kg'
	        WHEN b.product_name = 'Ghee' THEN 'Litres'
        END AS quantity_sold_unit,
        ISNULL(selling_price, 0) AS selling_price,
        COALESCE(revenue, 0) AS revenue, 
        COALESCE(total_value, 0) AS total_value,
        TRIM(COALESCE(customer_location, 'n/a')) AS customer_location, 
        TRIM(COALESCE(sales_channel, 'n/a')) AS sales_channel
        FROM bronze.dairy_info AS b
        INNER JOIN silver.products AS p
	        ON TRIM(b.product_name) = TRIM(p.product_name)
	        AND TRIM(b.brand) = TRIM(p.brand);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.inventory
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: silver.inventory';
        WITH ranked AS (
            SELECT
                p.product_key,
                d.stk_quantity,
                d.min_stk_threshold,
                d.reorder_quantity,
                d.product_name,

                ROW_NUMBER() OVER (
                    PARTITION BY p.product_key
                    ORDER BY d.stk_quantity DESC
                ) AS rn
            FROM bronze.dairy_info d
            INNER JOIN silver.products p
                ON TRIM(d.product_name) = TRIM(p.product_name)
               AND TRIM(d.brand) = TRIM(p.brand)
        )

        INSERT INTO silver.inventory (
            product_key,
            stock_value,
            stock_unit,
            min_stock_value,
            min_stock_unit,
            reorder_quantity_value,
            reorder_quantity_unit
        )
        SELECT
            product_key,
            COALESCE(stk_quantity, 0) AS stock_value,

            CASE
                WHEN product_name = 'Ice Cream' THEN 'Kg'
                WHEN product_name = 'Buttermilk' THEN 'Litres'
                WHEN product_name = 'Cheese' THEN 'Kg'
                WHEN product_name = 'Butter' THEN 'Kg'
                WHEN product_name = 'Lassi' THEN 'Litres'
                WHEN product_name = 'Yogurt' THEN 'Litres'
                WHEN product_name = 'Curd' THEN 'Litres'
                WHEN product_name = 'Milk' THEN 'Litres'
                WHEN product_name = 'Paneer' THEN 'Kg'
                WHEN product_name = 'Ghee' THEN 'Litres'
            END AS stock_unit,

            COALESCE(min_stk_threshold, 0) AS min_stock_value,

            CASE
                WHEN product_name = 'Ice Cream' THEN 'Kg'
                WHEN product_name = 'Buttermilk' THEN 'Litres'
                WHEN product_name = 'Cheese' THEN 'Kg'
                WHEN product_name = 'Butter' THEN 'Kg'
                WHEN product_name = 'Lassi' THEN 'Litres'
                WHEN product_name = 'Yogurt' THEN 'Litres'
                WHEN product_name = 'Curd' THEN 'Litres'
                WHEN product_name = 'Milk' THEN 'Litres'
                WHEN product_name = 'Paneer' THEN 'Kg'
                WHEN product_name = 'Ghee' THEN 'Litres'
            END AS min_stock_unit,

            COALESCE(reorder_quantity, 0) AS reorder_quantity_value,

            CASE
                WHEN product_name = 'Ice Cream' THEN 'Kg'
                WHEN product_name = 'Buttermilk' THEN 'Litres'
                WHEN product_name = 'Cheese' THEN 'Kg'
                WHEN product_name = 'Butter' THEN 'Kg'
                WHEN product_name = 'Lassi' THEN 'Litres'
                WHEN product_name = 'Yogurt' THEN 'Litres'
                WHEN product_name = 'Curd' THEN 'Litres'
                WHEN product_name = 'Milk' THEN 'Litres'
                WHEN product_name = 'Paneer' THEN 'Kg'
                WHEN product_name = 'Ghee' THEN 'Litres'
            END AS reorder_quantity_unit
        FROM ranked
        WHERE rn = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        
        SET @batch_end_time = GETDATE();
        PRINT '==============================';
        PRINT 'Loading Silver Layer is Complted';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==============================';

    END TRY
    BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
