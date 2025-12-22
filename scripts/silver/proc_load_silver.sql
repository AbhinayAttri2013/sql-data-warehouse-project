-----SCRIPT PURPOSE: THIS STORE PROCEDURE FOLLOWS THE ETL(EXTRACT, TRANSFORM, LOAD) PROCESS , 
----                  TO POPULATE THE 'SILVER' SCHEMA USING THE 'BRONZE' SCHEMA

---- ACTION PERFORMED: -TRUNCATE SILVER LAYER
---                    - INSERT, TRANSFORMATION, AND CLEANSING FROM  BRONZE INTO SILVER TABLE 
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE 
        @batch_start DATETIME2,
        @batch_end   DATETIME2,
        @step_start  DATETIME2,
        @step_end    DATETIME2;

    SET @batch_start = SYSDATETIME();

    PRINT '====================================================';
    PRINT 'STARTING SILVER LAYER LOAD PROCESS';
    PRINT 'Batch start time: ' + CONVERT(VARCHAR(30), @batch_start, 121);
    PRINT '====================================================';

    /* ================= CRM CUSTOMER ================= */

    PRINT 'STEP 1: Loading silver.crm_cust_info';
    SET @step_start = SYSDATETIME();
    PRINT 'Step start time: ' + CONVERT(VARCHAR(30), @step_start, 121);

    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
            WHEN UPPER(TRIM(cst_gndr)) = 'S' THEN 'SINGLE'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    SET @step_end = SYSDATETIME();
    PRINT 'Step completed. Duration (seconds): ' 
          + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR(10));
    PRINT '----------------------------------------------------';

    /* ================= CRM PRODUCT ================= */

    PRINT 'STEP 2: Loading silver.crm_prd_info';
    SET @step_start = SYSDATETIME();

    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm,
        prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)), '-', '_'),
        prd_nm,
        ISNULL(prd_cost, 0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'other Sales'
            WHEN 'T' THEN 'touring'
            ELSE 'N/A'
        END,
        CAST(prd_start_dt AS DATE),
        DATEADD(
            DAY, -1,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE)
        )
    FROM bronze.crm_prd_info;

    SET @step_end = SYSDATETIME();
    PRINT 'Step completed. Duration (seconds): ' 
          + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR(10));
    PRINT '----------------------------------------------------';

    /* ================= CRM SALES ================= */

    PRINT 'STEP 3: Loading silver.crm_sales_details';
    SET @step_start = SYSDATETIME();

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_sales <= 0 
               OR sls_sales IS NULL 
               OR sls_sales != sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
        END,
        sls_quantity,
        CASE WHEN sls_price <= 0 OR sls_price IS NULL
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    SET @step_end = SYSDATETIME();
    PRINT 'Step completed. Duration (seconds): ' 
          + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR(10));
    PRINT '----------------------------------------------------';

    /* ================= ERP TABLES ================= */

    PRINT 'STEP 4: Loading ERP tables';

    SET @step_start = SYSDATETIME();

    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (CID, BDATE, GEN)
    SELECT
        CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) ELSE CID END,
        CASE WHEN BDATE > GETDATE() THEN NULL ELSE BDATE END,
        CASE 
            WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
            WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (CID, CNTRY)
    SELECT
        REPLACE(CID, '-', ''),
        CASE 
            WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
            WHEN TRIM(CNTRY) IN ('USA','US') THEN 'United States'
            WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'N/A'
            ELSE TRIM(CNTRY)
        END
    FROM bronze.erp_loc_a101;

    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
    SELECT * FROM bronze.erp_px_cat_g1v2;

    SET @step_end = SYSDATETIME();
    PRINT 'ERP tables load completed. Duration (seconds): ' 
          + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR(10));

    /* ================= BATCH END ================= */

    SET @batch_end = SYSDATETIME();

    PRINT '====================================================';
    PRINT 'SILVER LAYER LOAD COMPLETED';
    PRINT 'Batch end time: ' + CONVERT(VARCHAR(30), @batch_end, 121);
    PRINT 'TOTAL DURATION (seconds): ' 
          + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS VARCHAR(10));
    PRINT '====================================================';
END;
