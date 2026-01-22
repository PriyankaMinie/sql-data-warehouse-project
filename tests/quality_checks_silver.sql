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
-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

-- crm_cust_info
-- check for duplicate records or NULL values
SELECT cst_id, count(*) FROM bronze.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL

-- check for unwanted spaces 
-- Expectation: No results

SELECT cst_gndr 
FROM bronze.crm_cust_info 
WHERE cst_gndr != TRIM(cst_gndr)

-- Data cleansing and writing insert query into silver level(clean) - crm_cust_info
-- removing unwanted spaces & data normalization/standarization & handling empty string or null & removing duplicates(only one record for each primary key, most relevant row)
PRINT '<<Truncating Table: silver.crm_cust_info>>'
TRUNCATE TABLE silver.crm_cust_info
PRINT '<<Inserting Data Into Table: silver.crm_cust_info>>'
INSERT INTO silver.crm_cust_info (cst_id,
cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
     WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
     ELSE 'n/a'
END AS cst_marital_status,
CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
     WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
     ELSE 'n/a'
END AS cst_gndr,
cst_create_date
FROM
(
SELECT *,
ROW_NUMBER () OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_latest
FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL
)t WHERE flag_latest = 1 


-- check the inserted data & data quality check

SELECT * FROM silver.crm_cust_info

SELECT cst_id, count(*) FROM silver.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT cst_firstname 
FROM silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname 
FROM silver.crm_cust_info 
WHERE cst_lastname != TRIM(cst_lastname)

SELECT DISTINCT cst_gndr 
FROM silver.crm_cust_info 

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

-- crm_prd_info 
-- quality check

SELECT * FROM bronze.crm_prd_info

SELECT * FROM bronze.erp_px_cat_g1v2

SELECT prd_id, count(*) FROM bronze.crm_prd_info GROUP BY prd_id HAVING COUNT(*) > 1 or prd_id IS NULL

SELECT * FROM bronze.crm_sales_details
-- check for unwanted spaces
SELECT prd_nm 
FROM bronze.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm)
-- check for NULLs or negative numbers
SELECT prd_cost 
FROM bronze.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL

-- check for invalid Date orders
SELECT * 
FROM bronze.crm_prd_info 
WHERE prd_start_dt > prd_end_dt


-- Data cleansing and writing insert query into silver level(clean) - crm_prd_info
-- removing unwanted spaces & data normalization/standarization & handling empty string or null & removing duplicates(only one record for each primary key, most relevant row)
PRINT '<<Truncating Table: silver.crm_prd_info>>'
TRUNCATE TABLE silver.crm_prd_info
PRINT '<<Inserting Data Into Table: silver.crm_prd_info>>'
INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
     WHEN  'M' THEN 'Mountain'
     WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'Other Sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

-- check data quality after insert:
--check for NULLs or duplicates in primary key

SELECT prd_id, count(*) FROM silver.crm_prd_info GROUP BY prd_id HAVING COUNT(*) > 1 or prd_id IS NULL

-- check for unwanted spaces
SELECT prd_nm 
FROM silver.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm)

-- data standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
-- check for NULLs or negative numbers
SELECT prd_cost 
FROM silver.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL

-- check for invalid Date orders
SELECT * 
FROM silver.crm_prd_info 
WHERE prd_start_dt > prd_end_dt

--  final check on the table
SELECT * FROM silver.crm_prd_info

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

-- crm_sales_details
-- data quality check
SELECT 
--sls_ord_num,
--sls_prd_key,
--sls_cust_id,
--NULLIF(sls_order_dt, 0) sls_order_dt,
--sls_ship_dt,
--sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details 
--WHERE sls_ord_num != TRIM(sls_ord_num)
--WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
--WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
--WHERE sls_order_dt <= 0 
--OR LEN(sls_order_dt) != 8 
--OR sls_order_dt >20500101 
--OR sls_order_dt < 19000101
--WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
-- <<check the business rules>>
-- sales = quantity * price
-- values must not be 0, NULL or negatives
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price


-- Data cleansing and writing insert query into silver level(clean) - crm_sales_details
-- removing unwanted spaces & data normalization/standarization & handling empty string or null & removing duplicates(only one record for each primary key, most relevant row)
PRINT '<<Truncating Table: silver.crm_sales_details>>'
TRUNCATE TABLE silver.crm_sales_details
PRINT '<<Inserting Data Into Table: silver.crm_sales_details>>'
INSERT INTO silver.crm_sales_details (
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     ELSE CAST(CAST (sls_order_dt AS VARCHAR) AS DATE) 
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     ELSE CAST(CAST (sls_ship_dt AS VARCHAR) AS DATE) 
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     ELSE CAST(CAST (sls_due_dt AS VARCHAR) AS DATE) 
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales !=sls_quantity * ABS(sls_price)
     THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
     THEN sls_sales / NULLIF(sls_quantity,0)
     ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details 


-- Check the health of the newly inserted data - silver.crm_sales_details

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details 
--WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
-- <<check the business rules>>
-- sales = quantity * price
-- values must not be 0, NULL or negatives
--WHERE sls_sales != sls_quantity * sls_price
--OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
--OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
--ORDER BY sls_sales, sls_quantity, sls_price

SELECT * FROM silver.crm_sales_details

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

-- erp_px_cat_g1v2
-- check for data quality
SELECT * FROM bronze.erp_cust_az12 WHERE cid LIKE '%AW00011006'

SELECT * FROM silver.crm_cust_info WHERE cst_key LIKE '%AW00011006'

SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

SELECT DISTINCT 
gen,
--CASE UPPER(TRIM(gen)) 
--     WHEN 'F' THEN 'Female'
--     WHEN 'Female' THEN 'Female'
--     WHEN 'Male' THEN 'Male'
--    WHEN 'M' THEN 'Male'
--     ELSE 'n/a'
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
     ELSE 'n/a'
END AS gen  
FROM bronze.erp_cust_az12


-- Data cleansing and writing insert query into silver level(clean) - erp_cust_az12
-- removing unwanted spaces & data normalization/standarization & handling empty string or null & removing duplicates(only one record for each primary key, most relevant row)
-- analyzing with the solution
SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
     ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
--WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
--           ELSE cid
--     END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- Creating final insert query
PRINT '<<Truncating Table: silver.erp_cust_az12>>'
TRUNCATE TABLE silver.erp_cust_az12
PRINT '<<Inserting Data Into Table: silver.erp_cust_az12>>'

INSERT INTO silver.erp_cust_az12 (
cid,
bdate,
gen
)
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) 
     ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate 
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
     ELSE 'n/a'
END AS gen     
FROM bronze.erp_cust_az12

-- Checking the health of newly inserted data

SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

SELECT DISTINCT 
gen
FROM silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12


-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

--erp_loc_a101
-- check data quality

SELECT 
cid,
cntry
FROM 
bronze.erp_loc_a101

SELECT 
cst_key
FROM silver.crm_cust_info

SELECT DISTINCT cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
     ELSE cntry
END cntry
FROM bronze.erp_loc_a101 

-- Data cleansing and writing insert query into silver level(clean) - erp_loc_a101
-- removing unwanted spaces & data normalization/standarization & handling empty string or null & removing duplicates(only one record for each primary key, most relevant row)
-- analyzing with the solution

SELECT 
REPLACE(cid, '-','') AS cid,
cntry
FROM 
bronze.erp_loc_a101
WHERE REPLACE(cid, '-','') NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info )


-- Final insert query
PRINT '<<Truncating Table: silver.erp_loc_a101>>'
TRUNCATE TABLE silver.erp_cust_az12
PRINT '<<Inserting Data Into Table: silver.erp_loc_a101>>'

INSERT INTO silver.erp_loc_a101 (
cid,
cntry
)
SELECT 
REPLACE(cid, '-','') AS cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
     ELSE cntry
END cntry
FROM 
bronze.erp_loc_a101

-- Health check after insertion
SELECT *
--cid,
--DISTINCT cntry 
FROM silver.erp_loc_a101

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

-- erp_px_cat_g1v2
-- check data quality

SELECT *
FROM bronze.erp_px_cat_g1v2

-- check unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- data standardization & Consistency
SELECT DISTINCT 
--cat
--subcat
maintenance
FROM bronze.erp_px_cat_g1v2

-- Final insert query
PRINT '<<Truncating Table: silver.erp_px_cat_g1v2>>'
TRUNCATE TABLE silver.erp_cust_az12
PRINT '<<Inserting Data Into Table: silver.erp_px_cat_g1v2>>'

INSERT INTO silver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance
)
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

-- Health check after insertion
SELECT * FROM silver.erp_px_cat_g1v2
