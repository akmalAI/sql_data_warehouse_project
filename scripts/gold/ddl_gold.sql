


IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
  DROP VIEW gold.dim_customers;

CREATE VIEW gold.dim_customers as 
  select
    -- generating surrogate key for golden layer
    ROW_NUMBER() OVER (order by cst_id) as customer_key,
    crc.cst_id as customer_id,
    crc.cst_key as customer_number,
    crc.cst_firstname as first_name,
    crc.cst_lastname as last_name,
    erl.cntry as country,
    crc.cst_marital_status as marital_status,
    CASE 
    	WHEN crc.cst_gndr !='n/a' THEN crc.cst_gndr
    	ELSE COALESCE(erc.gen,'n/a')
    END as gender,
    crc.cst_create_date as create_date,
    erc.bdate as birth_date
  from silver.crm_cust_info as crc
  left join silver.erp_cust_az12 as erc
    on crc.cst_key = erc.cid
  left join silver.erp_loc_a101 as erl
    on crc.cst_key = erl.cid

IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
  DROP VIEW gold.dim_products;

create view gold.dim_products as 
select 
  ROW_NUMBER() OVER(order by crpi.prd_start_dt, crpi.prd_key) as product_key ,-- created Surrogate key
  crpi.prd_id as product_id,
  crpi.prd_key as product_number,
  crpi.prd_nm as producst_name,
  crpi.category_id,
  erpx.cat as category,
  erpx.subcat as subcategory,
  erpx.maintenance,
  crpi.prd_cost as product_cost,
  crpi.prd_line as product_line,
  crpi.prd_start_dt as start_date
from silver.crm_prd_info as crpi
left join silver.erp_px_cat_g1v2 as erpx
  on crpi.category_id = erpx.id
  where prd_end_dt is NULL -- current products since they are active products

IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
  DROP VIEW gold.fact_sales;

create view gold.fact_sales as 
select 
  csd.sls_ord_num as order_number,
  gdp.product_key,
  gdc.customer_key,
  csd.sls_order_dt as order_date,
  csd.sls_ship_dt as ship_date,
  csd.sls_due_dt as due_date,
  csd.sls_sales as sales,
  csd.sls_quantity as quantity,
  csd.sls_price as price
from silver.crm_sales_details as csd
left join gold.dim_customers as gdc
  on csd.sls_cust_id = gdc.customer_id
left join gold.dim_products as gdp
  on csd.sls_prd_key = gdp.product_number;
