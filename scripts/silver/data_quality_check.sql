-- there is no issue on ID
select * from silver.erp_px_cat_g1v2 where id != trim(id);
-- extra spaces for category
select * from bronze.erp_px_cat_g1v2 where cat !=trim(cat);
select distinct cat from bronze.erp_px_cat_g1v2 where cat !=trim(cat);

-- extra spaces for sub category
select * from bronze.erp_px_cat_g1v2 where subcat !=trim(subcat);
select distinct subcat from bronze.erp_px_cat_g1v2 where subcat !=trim(subcat);

-- extra spaces for maintenance
select * from bronze.erp_px_cat_g1v2 where maintenance !=trim(maintenance);
select distinct maintenance from bronze.erp_px_cat_g1v2;


select * from silver.crm_prd_info;





select * from silver.erp_cust_az12;
select bdate from silver.erp_cust_az12 where bdate > getdate();
select * from bronze.crm_cust_info;
select distinct gen from silver.erp_cust_az12;

select * from bronze.crm_sales_details where sls_price < = 0;
select sls_ord_num , count(*) from bronze.crm_sales_details group by sls_ord_num having count(*) > 1;
-- sales amount not null
select * from bronze.crm_sales_details where sls_quantity is null or sls_quantity <= 0;
-- product is not empty
select * from bronze.crm_sales_details where sls_prd_key is null;
-- customer id is not null
select * from bronze.crm_sales_details where sls_cust_id is null;
-- check the dates are good 
select * from bronze.crm_sales_details where sls_ship_dt < sls_order_dt ;

-- check the order/ship/due date columns 1. shipment data is all good 2. order has issues with 0s 3. due date has not issues 
select sls_due_dt from bronze.crm_sales_details where sls_due_dt <= 0;


select * from silver.crm_sales_details where sls_sales != sls_price*sls_quantity or sls_sales is null or sls_price is null or sls_quantity is null
or sls_price < =0 or sls_quantity < = 0 or sls_sales <= 0;
truncate table silver.crm_sales_details;
