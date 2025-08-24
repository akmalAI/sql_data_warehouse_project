







create schema bronze;
go
create schema silver;
go
create schema gold;
go 

if OBJECT_ID ('bronze.crm_cust_info','U') is not null
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info(
 cst_id int,	cst_key	nvarchar(50), cst_firstname nvarchar(100),
 cst_lastname nvarchar(100),	cst_marital_status	nvarchar(50),
 cst_gndr nvarchar(50),	cst_create_date date 
)

select * from bronze.crm_cust_info;

if OBJECT_ID ('bronze.crm_prd_info','U') is not null
	drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id	int, prd_key nvarchar(100),
prd_nm nvarchar(100), prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
)

if OBJECT_ID ('bronze.crm_sales_details','U') is not null
	drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num	nvarchar(100),
sls_prd_key nvarchar(100),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
)

if OBJECT_ID ('bronze.erp_cust_az12','U') is not null
	drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
 cid nvarchar(100), 
 bdate date,
 gen nvarchar(100)
)

if OBJECT_ID ('bronze.erp_loc_a101','U') is not null
	drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid nvarchar(100),
cntry nvarchar(100)

)

if OBJECT_ID ('bronze.erp_px_cat_g1v2','U') is not null
	drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
id nvarchar(100),
cat nvarchar(100),
subcat nvarchar(100),
maintenance nvarchar(50)
)
