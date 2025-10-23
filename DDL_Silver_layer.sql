
----DDL_Script:Create Silver Tables-----------
===============================================================================================================================
Stored Procedure:Load Silver Layer (Bronze-> Silver)
===============================================================================================================================
 
Script Purpose:
This Stored Procedure performs the ETL (Extract,Transform,Load) procedure to populate the "silver_layer" schema tables from the "bronze_layer" schema

Action preferred:
Insert transformed and cleaned data from Bronze_layer to Silver_layer
===============================================================================================================================
 



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
