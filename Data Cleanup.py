# Databricks notebook source
# MAGIC %md
# MAGIC # Data Normalization Notebook
# MAGIC Cleanup raw data to load into silver db/schema. Cleanup process includes standardizing vendor names and rounding amounts to cents.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("data-cleanup").getOrCreate()

#df = spark.sql("SELECT vendor_name, contract_date, description_en, contract_value, buyer_name, owner_org_title FROM dbx_interview.default.contracts")

# COMMAND ----------

# Create database and table if they don't exist
CREATE_DB_SQL = """CREATE DATABASE IF NOT EXISTS dbx_interview.silver;"""

#CREATE_TABEL_SQL = """CREATE TABLE IF NOT EXISTS dbx_interview.silver.contracts 
#                       (vendor_name STRING, contract_date DATE, owner_org_title STRING, description_en STRING, original_value FLOAT) ;"""

spark.sql(CREATE_DB_SQL)
#spark.sql(CREATE_TABEL_SQL)

# COMMAND ----------

# Write to the silver db/schema after applying cleanup transformations (SQL only)
NORM_NAMES_SQL = """
select 
    case
      when upper(vendor_name) like 'AMAZON WEB SERVICES%' then "AMAZON WEB SERVICES"
      when upper(vendor_name) like 'GOOGLE CLOUD%' then "GOOGLE CLOUD"
      when upper(vendor_name) like 'MICROSOFT%' and description_en like 'Computer Services%' then "MICROSOFT"
      when upper(vendor_name) = 'IBM CANADA LTD' then 'IBM'
      when upper(vendor_name) = 'IBM GLOBAL BUSINESS SERVICES' then 'KYNDRYL'
      when upper(vendor_name) like '%KYNDRYL%' then 'KYNDRYL'
      when upper(vendor_name) like 'DELOITTE%' then 'DELOITTE'
      when upper(vendor_name) like 'PRICE%WATERHOUSE%' then 'PWC'
      when upper(vendor_name) like 'KPMG%' then 'KPMG'
      when upper(vendor_name) like 'ERNST & YOUNG%' then 'EY'
      when upper(vendor_name) like 'ORACLE%CANADA%' then 'ORACLE'
      when upper(vendor_name) like 'MCKINSEY%COMPANY%' then 'MCKINSEY'
      when upper(vendor_name) like 'SLALOM%' then 'SLALOM'
      when upper(vendor_name) like 'ACCENTURE%' then 'ACCENTURE'
      else upper(vendor_name)
    end as vendor_name,
  contract_date, 
  upper(owner_org_title) as owner_org_title,
  description_en as description,
  coalesce(try_cast(original_value as DOUBLE), 0) as original_value
from dbx_interview.default.contracts;"""

# Get cleaned output into df
norm_df = spark.sql(NORM_NAMES_SQL)

# Create table if it dosn't already exist. Use schema from df
if not spark.catalog.tableExists('dbx_interview.silver.contracts'):
  spark.catalog.createTable('dbx_interview.silver.contracts', schema=norm_df.schema)

# Write to table
norm_df.write.mode('overwrite').saveAsTable('dbx_interview.silver.contracts')
