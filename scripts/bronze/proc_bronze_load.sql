/*
==========================================
Stored Procedure: Load Bronze Layer (Source --> Bronze)
==========================================

Script Purpose: 
 	This stored procedure loads data into the 'bronze' schema from external CSV files.
	it performs the following actions:
		- Truncates the bronze tables before loading the data.
		- Uses the 'BULK INSERT' command to load data from csv files to bronze tables.
Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.
Usage Example:
	EXEC bronze.load_bronze;
============================================================================
*/

create or alter procedure bronze.load_bronze as
begin
 declare @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
 begin try
        
		set @batch_start_time = GETDATE();
		print '====== Load Data into Bronze Layer ======'

		print '--------- Load CRM data ---------' 

		print '>>>> Truncate Existing Data: bronze.crm_cust_info <<<<<'

		set @start_time = GETDATE();
		truncate table bronze.crm_cust_info;

		print'// Inserting New data into: bronze.crm_cust_info '
		bulk insert bronze.crm_cust_info 
		from 'C:\Users\akmal\OneDrive\Documents\DATA_ENGINEER_PREP\DWH_PROJECT_UDEMY\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			 firstrow = 2,
			 fieldterminator = ',',
			 tablock

		)
		
		set @end_time = GETDATE();
		print 'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		--select * from bronze.crm_cust_info;

		--select count(*) from bronze.crm_cust_info;
		set @start_time = GETDATE();
		print '>>>> Truncate Existing Data: bronze.crm_prd_info <<<<<'
		truncate table bronze.crm_prd_info;

		print'// Inserting New data into: bronze.crm_prd_info'
		bulk insert bronze.crm_prd_info
		from 'C:\Users\akmal\OneDrive\Documents\DATA_ENGINEER_PREP\DWH_PROJECT_UDEMY\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			 firstrow = 2,
			 fieldterminator = ',',
			 tablock

		)
		set @end_time = GETDATE();
		print 'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';

		--select * from bronze.crm_prd_info;
		set @start_time = GETDATE();
		print '>>>> Truncate Existing Data: bronze.crm_sales_details <<<<<'
		truncate table bronze.crm_sales_details;

		print'// Inserting New data into: bronze.crm_sales_details'
		bulk insert bronze.crm_sales_details
		from 'C:\Users\akmal\OneDrive\Documents\DATA_ENGINEER_PREP\DWH_PROJECT_UDEMY\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			 firstrow = 2,
			 fieldterminator = ',',
			 tablock

		)
		set @end_time = GETDATE();
		print 'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
		--select * from bronze.crm_sales_details;
		print '--------- Load ERP data ---------' 

		set @start_time = GETDATE();
		print '>>>> Truncate Existing Data: bronze.erp_cust_az12 <<<<<'

		truncate table bronze.erp_cust_az12;

		print'// Inserting New data into : bronze.erp_cust_az12 '
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\akmal\OneDrive\Documents\DATA_ENGINEER_PREP\DWH_PROJECT_UDEMY\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			 firstrow = 2,
			 fieldterminator = ',',
			 tablock

		)
		set @end_time = GETDATE();
		print 'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';

		--select * from bronze.erp_cust_az12;
		set @start_time = GETDATE();
		print '>>>> Truncate Existing Data: bronze.erp_loc_a101 <<<<<'
		truncate table bronze.erp_loc_a101;

		print'// Inserting New data into : bronze.erp_loc_a101'
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\akmal\OneDrive\Documents\DATA_ENGINEER_PREP\DWH_PROJECT_UDEMY\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			 firstrow = 2,
			 fieldterminator = ',',
			 tablock

		)
		set @end_time = GETDATE();
		print 'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';

		--select * from bronze.erp_loc_a101;
		set @start_time = GETDATE();
		print '>>>> Truncate Existing Data: bronze.erp_px_cat_g1v2 <<<<<'
		truncate table bronze.erp_px_cat_g1v2;

		print'Inserting New data into : bronze.erp_px_cat_g1v2 '
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\akmal\OneDrive\Documents\DATA_ENGINEER_PREP\DWH_PROJECT_UDEMY\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			 firstrow = 2,
			 fieldterminator = ',',
			 tablock

		)
		set @end_time = GETDATE();
		print 'Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';

		set @batch_end_time = GETDATE();
		print 'Batch Load Duration : ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + 'seconds';

		--select * from bronze.erp_px_cat_g1v2;
	end try
	begin catch
		 
		 print '========================';
		 print 'Error occured in data loading in bronze layer';
		 print 'Error occured ' +  error_message();
		 print 'Error occured ' + cast(error_number() as nvarchar);
		 print 'Error occured ' + cast(error_state() as nvarchar);
		 print '========================';
	  
	end catch

end;

exec bronze.load_bronze;
