
----DDL_Script:Create Silver Tables-----------
===============================================================================================================================
Stored Procedure:Load Silver Layer (Bronze-> Silver)
===============================================================================================================================
 
Script Purpose:
This Stored Procedure performs the ETL (Extract,Transform,Load) procedure to populate the "silver_layer" schema tables from the "bronze_layer" schema

Action preferred:
Insert transformed and cleaned data from Bronze_layer to Silver_layer
===============================================================================================================================



 -------------------------------------------------- DATA CLEANING--------------------------------------------------
------------------------------------------------- Table--crm_cust_info----------------------------------------------
use  bronze_layer;
select *from crm_cust_info;

-- By removing the duplicates in the primary key
-- Using bronze_layer,cleaning data there and transfering the cleaned data into the silver_layer
select cst_id,count(*) from crm_cust_info group by cst_id having count(*)>1;
select *from crm_cust_info  where cst_id =29433;
select *from (select *,row_number () over(partition by cst_id order by cst_create_date desc)
 as flag_last from crm_cust_info) as t  where flag_last=1 and cst_id=29433;
 
#check for unwanted spaces
select cst_firstname from crm_cust_info where cst_firstname!= trim(cst_firstname);
select cst_lastname from crm_cust_info where cst_lastname !=trim(cst_lastname);

#Removing the unwanted spaces from the (first+last)names
select cst_id,cst_key,cst_marital_status,cst_gndr,cst_create_date,
trim(cst_firstname) as cst_firstname,trim(cst_lastname) as cst_lastname from crm_cust_info;

#Data Standardisation
#Gender Standardisation
select  distinct(cst_gndr) from crm_cust_info;
select cst_id,cst_key,cst_marital_status, 
case when Upper(trim(cst_gndr))="M" THEN "Male" 
when Upper(trim(cst_gndr))="F" then "Female"
Else "n/a"
end as cst_gender,
cst_create_date,trim(cst_firstname) as cst_firstname,trim(cst_lastname) as cst_lastname from crm_cust_info;


#Marital_Staus Standardisation
select cst_id,cst_key, 
case when Upper(trim(cst_gndr))="M" THEN "Male" 
when Upper(trim(cst_gndr))="F" then "Female"
Else "n/a"
end as cst_gender,
case when upper(trim(cst_marital_status))="M" then "Married"
when upper(trim(cst_marital_status)) ="S" then "Single" 
else "n/a" 
end as cst_marital_status,
cst_create_date,trim(cst_firstname) as cst_firstname,trim(cst_lastname) as cst_lastname from crm_cust_info;


# Creating the Silver_layer database and inserting the cleaned data into it 
CREATE DATABASE IF NOT EXISTS silver_layer;
SHOW TABLES FROM bronze_layer;


#Table creation from broze_layer ti silver_layer
CREATE TABLE silver_layer.crm_sales_details LIKE bronze_layer.crm_sales_details;
CREATE TABLE silver_layer.crm_cust_info LIKE bronze_layer.crm_cust_info;
CREATE TABLE silver_layer.crm_prd_info LIKE bronze_layer.crm_prd_info;


show tables from silver_layer;
truncate table silver_layer.crm_cust_info;
select*from silver_layer.crm_cust_info;


-------------------------------------------- LOADING THE DATA FROM BRONZE_LAYER TO SILVER_LAYER (crm_cust_info)-------------------------------- 
------------------------------------- Insert from bronze_layer.crm_cust_info into silver_layer crm_cust_info-----------------
INSERT INTO silver_layer.crm_cust_info
(cst_id,cst_key,cst_gndr,cst_marital_status,cst_create_date,cst_firstname,cst_lastname)
SELECT cst_id,cst_key,
  CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	   WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
       ELSE 'n/a'
  END AS cst_gender,
  CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
       WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
       ELSE 'n/a'
  END AS cst_marital_status,
  cst_create_date,
  TRIM(cst_firstname),
  TRIM(cst_lastname)
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn FROM bronze_layer.crm_cust_info) AS t
WHERE rn = 1;


-----------------------------------  QUALITY CHECK (crm_cust_info)--------------------------------------
#Checking the cleaned data insertion 
use silver_layer;
select *from crm_cust_info;
select cst_id,count(*) from silver_layer.crm_cust_info group by cst_id having count(*)>1;
select *from (select *,row_number () over(partition by cst_id order by cst_create_date desc)
as flag_last from crm_cust_info) as t  where flag_last=1 and cst_id=29466;
select count(*) from silver_layer.crm_cust_info;
select cst_firstname from silver_layer.crm_cust_info where cst_firstname!=trim(cst_firstname);
select cst_lastname from silver_layer.crm_cust_info where cst_lastname!=trim(cst_lastname);
select distinct cst_gndr from silver_layer.crm_cust_info;
select distinct cst_marital_status from silver_layer.crm_cust_info;








------------------------------------------------------- DATA CLEANING FOR TABLE crm_prd_info--------------------------------------------------
------------------------------------------------------- Table--crm_prd_info--------------------------------------------------------------
use bronze_layer;
select *from bronze_layer.crm_prd_info;

-- Check for duplicates(prd_id)
select prd_id,count(*) from bronze_layer.crm_prd_info group by prd_id having count(*) >1 and prd_id is null;

-- Setting relationship between prd_key of crm_prd_info and id of erp_px_cat_g1v2 table
select *from crm_prd_info;
select prd_id,prd_key,replace(substring(prd_key,1,5),"-","_") as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt from crm_prd_info;

-- check for extra unwanted spaces 
select prd_nm from bronze_layer.crm_prd_info where prd_nm!=trim(prd_nm);

-- check for Nulls or Negative cost 
select prd_cost from bronze_layer.crm_prd_info where prd_cost<0 or prd_cost is null;
UPDATE bronze_layer.crm_prd_info SET prd_cost = NULL WHERE TRIM(prd_cost) = '';

-- change the null to zero 
UPDATE bronze_layer.crm_prd_info SET prd_cost = 0 WHERE TRIM(prd_cost) is null;
-- "OR"--
select prd_id,prd_key,replace(substring(prd_key,1,5),"-","_") as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,
ifnull(prd_cost,0) as prd_cost,
prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt from bronze_layer.crm_prd_info;

 -- prd_start_dt,prd_end_dt
 select prd_id,prd_key,prd_nm,prd_cost,prd_start_dt,
 date_sub(lead(prd_start_dt) over(partition by prd_KEY order by prd_start_dt),INTERVAL 1 DAY)
 as prd_end_dt_test
 from bronze_layer.crm_prd_info where prd_key in ("AC-HE-HL-U509-R","AC-HE-HL-U509");

-- Change the abrrevation of prd_line
select prd_id,replace(substring(prd_key,1,5),"-","_") as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,prd_nm,
ifnull(prd_cost,0) as prd_cost,
 case when Upper(trim(prd_line))="M" then "Mountain"
 when Upper(trim(prd_line))="R" then "Road"
 when Upper(trim(prd_line))="S" then "Other Sales"
 when upper(trim(prd_line))="T" then "Touring"
 else "n/a"
 end as prd_line,
 prd_start_dt,
 date_sub(lead(prd_start_dt) over(partition by prd_KEY order by prd_start_dt),INTERVAL 1 DAY)
 as prd_end_dt from bronze_layer.crm_prd_info;
 
 ---------------------------------- LOADING THE DATA FROM BRONZE_LAYER TO SILVER_LAYER(crm_prd_info)------------------------------------------------------
  
 INSERT INTO silver_layer.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
 select prd_id,replace(substring(prd_key,1,5),"-","_") as cat_id,
SUBSTRING(prd_key,7,length(prd_key)) as prd_key,prd_nm,
ifnull(prd_cost,0) as prd_cost,
 case when Upper(trim(prd_line))="M" then "Mountain"
 when Upper(trim(prd_line))="R" then "Road"
 when Upper(trim(prd_line))="S" then "Other Sales"
 when upper(trim(prd_line))="T" then "Touring"
 else "n/a"
 end as prd_line,
 prd_start_dt,
 date_sub(lead(prd_start_dt) over(partition by prd_KEY order by prd_start_dt),INTERVAL 1 DAY)
 as prd_end_dt from bronze_layer.crm_prd_info;
 
 
 ---------------------------------------------------- QUALITY CHECKS FOR LOADED DATA INTO SILVER_LAYER (crm_prd_info) -----------------------------------------
 -- Checks for nulls and duplicates in primary key
 -- Expectation:No result
 select *from silver_layer.crm_prd_info;
 select prd_id,count(*) from silver_layer.crm_prd_info group by prd_id having count(*)>1;


-- check for unwanted spaces
-- expectaion:No result
select prd_nm from silver_layer.crm_prd_info where prd_nm!=trim(prd_nm);

-- check for Nulls or Negative Numbers
-- Expeactation:-No Result
select prd_cost from silver_layer.crm_prd_info where prd_cost <0 or prd_cost is null;

-- check for Data Standardization and Data consistency
select distinct prd_line from silver_layer.crm_prd_info;

-- check for invalid date orders
select *from silver_layer.crm_prd_info where prd_end_dt<prd_start_dt;

---------------------------------------------------- TYPES OF DATA CLEANING/DATA TRANSFORMATIONS  DONE (crm_prd_info)----------------------------------------

-- Derived Columns such as cat_id,*prd_key(new)from prd_key(old)
-- Handled missing values (prd_cost,if null replaced by 0)
-- Data Normalisation in prd_line
-- Data Enrichment(added new value of start_dte,end_dte





------------------------------------------------------ DATA CLEANING FOR (sls_sales_details) ------------------------------------------------------------

select sls_prd_key from bronze_layer.crm_sales_details where sls_prd_key="BC-M005";

-- Check the date_format 
select nullif(sls_order_dt,0) from bronze_layer.crm_sales_details where sls_order_dt<=0; 

-- Check if any sls_order_date has length more than "YYYY-MM-DD"
select  sls_ord_num,sls_prd_key,sls_cust_id,case when length(sls_order_dt)!=8 or sls_order_dt=0 then null
else cast(sls_order_dt  as Date)end as sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price
 from bronze_layer.crm_sales_details;
 
 -- Check if any sls_ship_date has length more than "YYYY-MM-DD"
select  sls_ord_num,sls_prd_key,sls_cust_id,case when length(sls_order_dt)!=8 or sls_order_dt=0 then null
else cast(sls_order_dt  as Date)end as sls_order_dt, case when length(sls_ship_dt)!=8 or sls_ship_dt=0 then null
else cast(sls_ship_dt as Date) end as sls_ship_date,sls_due_dt,sls_sales,sls_quantity,sls_price
 from bronze_layer.crm_sales_details;
 
-- Check if any sls_due_date has length more than "YYYY-MM-DD"
select  sls_ord_num,sls_prd_key,sls_cust_id,case when length(sls_order_dt)!=8 or sls_order_dt=0 then null
else cast(sls_order_dt  as Date)end as sls_order_dt, case when length(sls_ship_dt)!=8 or sls_ship_dt=0 then null
else cast(sls_ship_dt as Date) end as sls_ship_date,
case when length(sls_due_dt)!=8 or sls_due_dt=0 then null else cast(sls_due_dt as date)end as sls_due_dt,sls_sales,sls_quantity,sls_price
 from bronze_layer.crm_sales_details;
 
 -- Check for invalid dates (order_date should be smaller than shipping_date)
 select *from bronze_layer.crm_sales_details where sls_order_dt>sls_ship_dt;
 
 -- Check Data consistency:betwen sales,quantity,price
 -- Sales=Quantity*Price
 -- Values should not be Null,zero,Negative
 select  sls_ord_num,sls_prd_key,sls_cust_id,case when length(sls_order_dt)!=8 or sls_order_dt=0 then null
else cast(sls_order_dt  as Date)end as sls_order_dt, case when length(sls_ship_dt)!=8 or sls_ship_dt=0 then null
else cast(sls_ship_dt as Date) end as sls_ship_date,
case when length(sls_due_dt)!=8 or sls_due_dt=0 then null else cast(sls_due_dt as date)end as sls_due_dt,
case when sls_sales is null or sls_sales <=0 or sls_sales!=abs(sls_price)*sls_quantity
 then abs(sls_price)*sls_quantity else sls_sales end as sls_sales,sls_quantity,
 case when sls_price is null or sls_price<=0 
 then sls_sales/(nullif(sls_quantity,0))
 else sls_price
 end as sls_price from bronze_layer.crm_sales_details ;
 
 ---------------------------------------------------------- LOADING THE CLEANED DATA from bronze_layer INTO  silver_layer.crm_sales_details----------------------------------
 
INSERT INTO silver_layer.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
SELECT  
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  CASE 
    WHEN LENGTH(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL
    ELSE  sls_order_dt  
  END AS sls_order_dt,

  CASE 
    WHEN LENGTH(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL
    ELSE sls_ship_dt
  END AS sls_ship_dt,

  CASE 
    WHEN LENGTH(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL
    ELSE sls_due_dt 
  END AS sls_due_dt,

  CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_price) * sls_quantity
    THEN ABS(sls_price) * sls_quantity
    ELSE sls_sales
  END AS sls_sales,

  sls_quantity,

  CASE 
    WHEN sls_price IS NULL OR sls_price <= 0 
    THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
  END AS sls_price
FROM bronze_layer.crm_sales_details;

-- Since dates column were truncated so now load un the form of int then modify it permananetly yo date 
DESC silver_layer.crm_sales_details;
-- shows that date column is in int form---convert it into date format---- ("YY-MM-DD")
ALTER TABLE silver_layer.crm_sales_details
MODIFY COLUMN sls_order_dt DATE,
MODIFY COLUMN sls_ship_dt DATE,
MODIFY COLUMN sls_due_dt DATE;
SELECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM silver_layer.crm_sales_details
LIMIT 10;


--------------------------------------------------------------- QUALITY CHECK--------------------------------------------------------------------
---------------------------------------------------------------- sls_order_details---------------------------------------
use silver_layer ; 
-- Check for invalid  date orders
Select *from silver_layer.crm_sales_details where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt;

-- Check for Data consistency betwwen sales,quantity,price
-- >> Sales=quantuty*price
-- >> values must be nULL OR positive
 
select distinct sls_sales,sls_quantity,sls_price from silver_layer.crm_sales_details 
where sls_sales!= sls_quantity*sls_price or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales<=0 or sls_quantity<=0 or sls_price<=0 order by sls_sales,sls_quantity,sls_price;

------------------------------------------------------------------- TYPES OF DATA TRANSFORMATIONS DONE ------------------------------------------------
-- Handling Nulls
-- Hnadling Invalid data by doing Data transformations
-- Dta type casting 

 



---------------------------------------------------- DATA CLEANING--------------------------------------------------------------
--------------------------------------------------- TABLE:erp_cust_az12 --------------------------------------------------------
select *from bronze_layer.erp_cust_az12;
select *from bronze_layer.crm_cust_info;

-- Relationship betwen cid and cst_key--
Select case when cid like "NAS%" then substring(cid,4,length(cid)) else cid end as cid,bdate,gen from bronze_layer.erp_cust_az12;

--- clean bdate 
select case when bdate <"1924-01-01" or bdate> current_date() then null else bdate end as bdate,gen from bronze_layer.erp_cust_az12;

-- Data  Standardisation and Consistency
select distinct gen from bronze_layer.erp_cust_az12;
select distinct gen, case when upper(trim(gen)) in ("M","Male") then "Male" 
when upper(trim(gen)) in ("F","Female") then "Female" else "n/a" 
end as gen from bronze_layer.erp_cust_az12;

--------------------------------------------------- LOADING INTO Silver_layer----------------------------------------------------------------
---------------------------------------------------- (erp_cust_az12)--------------------------------------------------------------------------

CREATE TABLE silver_layer.erp_cust_az12 (cid VARCHAR(50),bdate DATE,gen VARCHAR(10));

Insert into silver_layer.erp_cust_az12 (cid,bdate,gen)
Select case when cid like "NAS%" then substring(cid,4,length(cid)) else cid end as cid,
 case when bdate <"1924-01-01" or bdate> current_date() then null else bdate end as bdate,
 case when upper(trim(gen)) in ("M","Male") then "Male" 
when upper(trim(gen)) in ("F","Female") then "Female" else "n/a" 
end as gen from bronze_layer.erp_cust_az12;

select *from erp_cust_az12;

------------------------------------------------------------ QUALITY CHECK -----------------------------------------------------------------------
--- Identify out of range dates
select distinct bdate from silver_layer.erp_cust_az12 where bdate<"1924-01-01" or bdate> current_date();

--- Data Standardisation 
select distinct gen from silver_layer.erp_cust_az12;

------------------------------------ TYPES OF DATA CLEANING/DATA TRANSFORMATIONS  DONE (erp_cust_az12)----------------------------------------
-- Handled Invalid data 
-- Data Transformations







--------------------------------------------------------------DATA CLEANING ----------------------------------------------------------------
--------------------------------------------------------------- TABLE:-erp_loc_a101----------------------------------------------------------
select *from bronze_layer.erp_loc_a101;

--- Setting Relationship between cid and cst_key
Select replace(CID,"-",""),cntry from bronze_layer.erp_loc_a101;

--- Data Standardisation and Consistency
Select case when upper(trim(cntry)) in ("US","USA") then "United States"
when upper(trim(cntry))="DE" then " Germany"
when upper(trim(cntry))="" or upper(trim(cntry)) is null then "n/a" 
else (trim(cntry))
end as  cntry from bronze_layer.erp_loc_a101;

------------------------------------------------------------------ LOADING INTO SILVER_LAYER-------------------------------------------------
-------------------------------------------------------------------- erp_loc_a101---------------------------------------------------------------

CREATE TABLE silver_layer.erp_loc_a101 (cid VARCHAR(50),cntry VARCHAR(90));

Insert into silver_layer.erp_loc_a101 (cid,cntry)
Select replace(CID,"-",""),
 case when upper(trim(cntry)) in ("US","USA") then "United States"
when upper(trim(cntry))="DE" then " Germany"
when upper(trim(cntry))="" or upper(trim(cntry)) is null then "n/a" 
else (trim(cntry))
end as cntry from bronze_layer.erp_loc_a101;


------------------------------------------------------------------------- QAULITY CHECK--------------------------------------------------------------
--- Data Standardisation 
Select distinct cntry from silver_layer.erp_loc_a101;
select *from silver_layer.erp_loc_a101;


------------------------------------ TYPES OF DATA CLEANING/DATA TRANSFORMATIONS  DONE (erp_loc_a101)----------------------------------------
-- Handled Invalid data 
-- Data Normalisation
-- Handled Missing Values
-- Removed Unwanted Spaces








---------------------------------------------------------------DATA CLEANING--------------------------------------------------------
-------------------------------------------------------------- TABLE:-erp_px_g1v2---------------------------------------------------
--- Check for unwanted Spaces
Select *from bronze_layer.erp_px_cat_g1v2 
where cat!=trim(cat) or subcat!=trim(subcat) or maintenance!=trim(maintenance);

--- Data Standardisation & Consistency
Select distinct cat,subcat,maintenance from bronze_layer.erp_px_cat_g1v2 ;

------------------------------------------------------------- LOADING INTO SILVER LAYER-----------------------------------------------
-------------------------------------------------------------- TABLE:-erp_px_cat_g1v2---------------------------------------------------
CREATE TABLE silver_layer.erp_px_cat_g1v2 (id VARCHAR(50),cat VARCHAR(90),subcat varchar(90),maintenance varchar(80));

Insert into silver_layer.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
(select id,cat,subcat,maintenance from bronze_layer.erp_px_cat_g1v2)
