# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,BooleanType,DateType



# COMMAND ----------

hub_order = spark.read.table("hive_metastore.data_vault_retail.hub_order")
sat_order = spark.read.table("hive_metastore.data_vault_retail.sat_order_bv")
deleted_orders = spark.read.table("hive_metastore.data_vault_retail.sat_order_record_status").filter("order_record_state = 'DELETED'").select("order_id_hash_key","order_record_state")

ret_df = hub_order.join(deleted_orders,["order_id_hash_key"],"left").filter("order_record_state is NULL").join(sat_order,["order_id_hash_key"],"inner").withColumnRenamed("order_id_hash_key","order_dim_key")

# COMMAND ----------

hh = hub_order.join(deleted_orders,["order_id_hash_key"],"left").filter("order_record_state is NULL")

# COMMAND ----------

hh.count()

# COMMAND ----------

hh2 = hub_order.join(deleted_orders,["order_id_hash_key"],"left").filter("order_record_state is NULL").join(sat_order,["order_id_hash_key"],"inner")

# COMMAND ----------

hh2.count()

# COMMAND ----------

display(hh)

# COMMAND ----------

display(hh2)

# COMMAND ----------

ret_df.count()

# COMMAND ----------

hub_order.count()

# COMMAND ----------

deleted_orders.count()

# COMMAND ----------

sat_order.count()

# COMMAND ----------

sat_order2.count()

# COMMAND ----------

hub_customer = spark.read.table("hive_metastore.data_vault_retail.hub_customer")
sat_customer_personal = spark.read.table("hive_metastore.data_vault_retail.sat_customer_personal_detail").select("customer_id_hash_key",concat(col("first_name"), lit(" "), col("last_name")).alias("name"),"dob")
sat_customer_address = spark.read.table("hive_metastore.data_vault_retail.sat_customer_address").select("customer_id_hash_key",col("load_timestamp").alias("cust_addr_load_timestamp"),"city","state","country")
    
ret_df = hub_customer.join(sat_customer_personal,["customer_id_hash_key"],"inner").join(sat_customer_address,["customer_id_hash_key"],"inner").select(sha1(concat("customer_id","cust_addr_load_timestamp")).alias("customer_dim_key"),"customer_id","cust_addr_load_timestamp","name","dob","city","state","country")

# COMMAND ----------

hub_customer.count()

# COMMAND ----------

sat_customer_personal.count()

# COMMAND ----------

sat_customer_address.count()

# COMMAND ----------

ret_df.count()

# COMMAND ----------

display(ret_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_vault_retail.dim_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_vault_retail.sat_customer_address

# COMMAND ----------


