-- Databricks notebook source
-- MAGIC %sql
-- MAGIC show databases;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create database data_vault_retail;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select unix_timestamp('9099-01-01 00:00:00','yyyy-MM-dd HH:mm:ss')

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC use data_vault_retail

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC show tables

-- COMMAND ----------

SELECT * from hub_customer

-- COMMAND ----------

select * from sat_customer_address

-- COMMAND ----------

select * from sat_customer_personal_detail

-- COMMAND ----------

select * from customer_raw_temp

-- COMMAND ----------

select * from order_raw_temp

-- COMMAND ----------

SELECT * from dim_order

-- COMMAND ----------

select * from sat_customer_personal_detail where __END_AT IS not NULL

-- COMMAND ----------


@dlt.table(
    name = "dim_customer"
)
def dim_customer():
    hub_customer = dlt.read("hub_customer")
    sat_customer_personal = dlt.read("sat_customer_personal_detail")
    sat_customer_address = dlt.read("sat_customer_address")
    
    ret_df = hub_customer.join(sat_customer_personal,["customer_id_hash_key"],"inner") \
    .join(sat_customer_address,["customer_id_hash_key"],"inner") \
    
    return(
        ret_df
    )

-- COMMAND ----------

select * from customer_raw_temp

-- COMMAND ----------

select * from dim_customer

-- COMMAND ----------

select * from sat_order

-- COMMAND ----------

describe sat_order

-- COMMAND ----------

select * from fact_order ORDER BY cust_addr_load_timestamp, customer_id_hash_key, order_id_hash_key

-- COMMAND ----------

select * from dim_order

-- COMMAND ----------

select f.*, dc.* from fact_order f join dim_customer dc on f.customer_dim_key = dc.customer_dim_key

-- COMMAND ----------

select f.*, do.* from fact_order f join dim_order do on f.order_id_hash_key = do.order_dim_key

-- COMMAND ----------

describe HISTORY fact_order

-- COMMAND ----------

select h.customer_id,a.* from sat_customer_address a join hub_customer h  on a.customer_id_hash_key = h.customer_id_hash_key ORDER BY a.load_timestamp

-- COMMAND ----------

select * from dim_customer 

-- COMMAND ----------

select * from hub_order

-- COMMAND ----------

select * from link_customer_order

-- COMMAND ----------

select * from sat_order_record_status

-- COMMAND ----------




-- COMMAND ----------

select * from sat_order

-- COMMAND ----------

select * from sat_order_bv

-- COMMAND ----------

drop table sat_order_bv

-- COMMAND ----------

select * from order_raw_temp

-- COMMAND ----------


