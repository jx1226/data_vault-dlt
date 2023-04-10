-- Databricks notebook source


-- COMMAND ----------

CREATE OR REFRESH STREAMING Live TABLE customer_raw
AS SELECT *
  FROM cloud_files(
    "/user/data-vault/retail/customer",
    "parquet")

-- COMMAND ----------

create or refresh STREAMING LIVE  view customer_new as select  case when Op_Type == 'c' then sha1(string(after.Customer_Id)) else sha1(string(before.Customer_Id)) end ;  customer_id_hash_key, current_timestamp() load_time,'retail' record_resouce, after.customer_id customer_id,  from stream(live.customer_raw) where op_type = 'c'

-- COMMAND ----------


