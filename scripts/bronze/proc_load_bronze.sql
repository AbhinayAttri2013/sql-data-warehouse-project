/* 
=====================
Stored Procedure: This loads data into  the bronze layer
=====================
=====================
Script: This stored procedure is used to load data from the external CSV file.
         It does the following functions :
        -Truncate the table before loading the data.
        -It uses the bulk insert to load data from csv file into the bronze tables.
*\


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME2,
            @end_time   DATETIME2,
            @batch_start DATETIME2,
            @batch_end   DATETIME2,
            @msg NVARCHAR(200);

    ---------------------------------------------------------
    -- Start total batch timer
    ---------------------------------------------------------
    SET @batch_start = SYSDATETIME();

    BEGIN TRY
        PRINT '==============================================';
        PRINT '   STARTING BRONZE LAYER LOAD PROCESS';
        PRINT '==============================================';

        -----------------------------
        -- CRM: cust_info
        -----------------------------
        PRINT '>>> Loading CRM: cust_info';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\sql project file\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '    Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        WAITFOR DELAY '00:00:00.1';


        -----------------------------
        -- CRM: prd_info
        -----------------------------
        PRINT '>>> Loading CRM: prd_info';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\sql project file\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '    Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        WAITFOR DELAY '00:00:00.1';


        -----------------------------
        -- CRM: sales_details
        -----------------------------
        PRINT '>>> Loading CRM: sales_details';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\sql project file\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '    Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        WAITFOR DELAY '00:00:00.1';


        PRINT '----------------------------------------------';
        PRINT '   LOADING ERP TABLES';
        PRINT '----------------------------------------------';


        -----------------------------
        -- ERP: cust_az12
        -----------------------------
        PRINT '>>> Loading ERP: cust_az12';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\sql project file\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '    Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        WAITFOR DELAY '00:00:00.1';


        -----------------------------
        -- ERP: loc_a101
        -----------------------------
        PRINT '>>> Loading ERP: loc_a101';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\sql project file\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '    Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        WAITFOR DELAY '00:00:00.1';


        -----------------------------
        -- ERP: px_cat_g1v2
        -----------------------------
        PRINT '>>> Loading ERP: px_cat_g1v2';
        SET @start_time = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\sql project file\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @end_time = SYSDATETIME();
        PRINT '    Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) + ' seconds';
        WAITFOR DELAY '00:00:00.1';


        ---------------------------------------------------------
        -- END OF BATCH — CALCULATE TOTAL DURATION
        ---------------------------------------------------------
        SET @batch_end = SYSDATETIME();

        PRINT '==============================================';
        PRINT '   BRONZE LAYER LOAD COMPLETED SUCCESSFULLY';
        PRINT '   TOTAL BATCH DURATION: ' 
              + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR(20))
              + ' seconds';
        PRINT '==============================================';
    END TRY

    BEGIN CATCH
        PRINT '==============================================';
        PRINT '   ERROR OCCURRED DURING BRONZE LOAD';
        PRINT '   MESSAGE: ' + ERROR_MESSAGE();
        PRINT '   NUMBER : ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
        PRINT '==============================================';
    END CATCH
END;
GO
