# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,BooleanType,DateType

# COMMAND ----------

dbutils.widgets.text("base_path", "abfss://dlt@rawdata18042024.dfs.core.windows.net/retail")
base_path = dbutils.widgets.get("base_path")

dbutils.widgets.text("record_source", "retail_global")
record_source = dbutils.widgets.get("record_source")

# COMMAND ----------

customer_path = f"{base_path}/customer"
order_path = f"{base_path}/order"

# COMMAND ----------

@dlt.table
def customer_raw():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .load(customer_path)
  )

# COMMAND ----------

#In this table it determines if contact or address records are changed, sometimes in updates only address or contact information may change
@dlt.table(
    name = "customer_raw_temp",
    temporary = True 
)
def customer_raw_temp():
    return (
        dlt.readStream("customer_raw")
        .withColumn("customer_id",when((col("Op_Type") == lit('c')) | (col("Op_Type") == lit('u')), col("after.Customer_ID")).otherwise(col("before.Customer_ID")))
        .withColumn("customer_id_hash_key",when((col("Op_Type") == lit('c')) | (col("Op_Type") == lit('u')),sha1(col("after.Customer_ID").cast(StringType()))).otherwise(sha1(col("before.Customer_ID").cast(StringType()))))
        .withColumn("customer_address_change",when(col("Op_Type") == lit('c'),lit("new")).when((col("Op_Type") == lit('u')) & ( sha1(concat("before.Addr_Line1","before.Addr_Line2","before.ZipCode","before.country") )!= sha1(concat("after.Addr_Line1","after.Addr_Line2","after.ZipCode","after.country") ) ),lit("update")).otherwise(lit("no_change")))
        .withColumn("customer_contact_change",when(col("Op_Type") == lit('c'),lit("new")).when((col("Op_Type") == lit('u')) & ( sha1(concat("before.Phone","before.mobile","before.email") )!= sha1(concat("after.Phone","after.mobile","after.email") ) ),lit("update")).otherwise(lit("no_change")))
        .withColumn("record_source",lit(record_source))
        .withColumn("load_timestamp",from_unixtime("Op_TimeStamp"))
    )

# COMMAND ----------

@dlt.table(
    name = "hub_customer"
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def hub_customer():
    return (
        dlt.readStream("customer_raw_temp")
        .filter("Op_Type == 'c'")
        .select("customer_id_hash_key","load_timestamp","record_source","customer_id")
    )

# COMMAND ----------

#Assumption: expecting that personal detail of the customer does not changed after creating, otherwise we can follow the pattern of merging the satellite from temporary table demonstrated for customer_address and customer_phone
@dlt.table(
    name = "sat_customer_personal_detail"
)
def sat_customer_personal_detail():
    return (
        dlt.readStream("customer_raw_temp")
        .filter("customer_id IS NOT NULL")
        .filter("Op_Type = 'c'")
        .select("customer_id_hash_key","load_timestamp","record_source",col("after.First_Name").alias("first_name"),col("after.Last_Name").alias("last_name"),col("after.Gender").alias("gender"),col("after.DOB").alias("dob"))
    )

# COMMAND ----------

#This table is created to perform the merge to the actual sat_customer_address table, this temp table will store only incremental insert/updates and merge those to actual satellite table. As this is temp table, this data will not be persisted after DLT run.
@dlt.table(
    name = "sat_customer_address_temp",
    temporary = True
)
def sat_customer_address_temp():
    return (
        dlt.readStream("customer_raw_temp")
        .filter("customer_id IS NOT NULL")
        .filter("customer_address_change = 'new' or customer_address_change = 'update'")
        .select("customer_id_hash_key","load_timestamp","record_source",col("after.Addr_Line1").alias("addr_line1"), \
            col("after.Addr_Line2").alias("addr_line2"),col("after.city").alias("city"),col("after.zipcode").alias("zipcode"), \
                col("after.state").alias("state"),col("after.country").alias("country"),"Op_TimeStamp")
    )

# COMMAND ----------

dlt.create_streaming_live_table("sat_customer_address")

dlt.apply_changes(
    target = "sat_customer_address",
    source = "sat_customer_address_temp",
    keys = ["customer_id_hash_key"],
    sequence_by = col("Op_TimeStamp"),
    except_column_list = ["Op_TimeStamp"],
    stored_as_scd_type = 2
)

# COMMAND ----------

#This table is created to perform the merge to the actual sat_customer_address table, this temp table will store only incremental insert/updates and merge those to actual satellite table. As this is temp table, this data will not be persisted after DLT run.
@dlt.table(
    name = "sat_customer_contact_temp",
    temporary = True
)
def sat_customer_address_temp():
    return (
        dlt.readStream("customer_raw_temp")
        .filter("customer_id IS NOT NULL")
        .filter("customer_contact_change = 'new' or customer_contact_change = 'update'")
        .select("customer_id_hash_key","load_timestamp","record_source",col("after.Phone").alias("phone"),col("after.mobile").alias("mobile"),col("after.email").alias("email"),"Op_TimeStamp")
    )

# COMMAND ----------

dlt.create_streaming_live_table("sat_customer_contact")

dlt.apply_changes(
    target = "sat_customer_contact",
    source = "sat_customer_contact_temp",
    keys = ["customer_id_hash_key"],
    sequence_by = col("Op_TimeStamp"),
    except_column_list = ["Op_TimeStamp"],
    stored_as_scd_type = 2
)

# COMMAND ----------

@dlt.table
def order_raw():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .load(order_path)
  )

# COMMAND ----------


@dlt.table(
    name = "order_raw_temp"
    ,temporary = True
)
def order_raw_temp():
    return (
        dlt.readStream("order_raw")
        .withColumn("order_id",when((col("Op_Type") == lit('c')) | (col("Op_Type") == lit('u')), col("after.Order_Id")).otherwise(col("before.Order_Id")))
        .withColumn("order_id_hash_key",sha1(col("order_id").cast(StringType())))
        .withColumn("customer_id",when((col("Op_Type") == lit('c')) | (col("Op_Type") == lit('u')), col("after.customer_Id")).otherwise(col("before.customer_Id")))
        .withColumn("customer_id_hash_key",sha1(col("customer_id").cast(StringType())))
        .withColumn("customer_id_order_id_hash_key",sha1(concat("order_id","customer_id")))
        .withColumn("record_source",lit(record_source))
        .withColumn("load_timestamp",from_unixtime("Op_TimeStamp"))
    )

# COMMAND ----------

@dlt.table(
    name = "hub_order"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
def hub_order():
    return (
        dlt.readStream("order_raw_temp")
        .filter("Op_Type = 'c' ")
        .select("order_id_hash_key","load_timestamp","record_source","order_id")
    )

# COMMAND ----------

#Assumption:- Business expectation is that Order is either created or deleted, never updated so not creating any SCD type 2 table for sat_order, if it gets updated then pattern of customer satelites can be used for order satellite table as well. 
@dlt.table(
    name = "sat_order"
)
def sat_order():
    return(
        dlt.readStream("order_raw_temp")
        .filter("order_id IS NOT NULL")
        .filter("Op_Type = 'c' ")
        .withColumn("order_date",to_timestamp(col("after.Order_Date"),'yyyy-MM-dd HH:mm:ss'))
        .withColumn("order_status",col("after.Order_Status"))
        .withColumn("gross_amount",col("after.Order_Gross_Amount"))
        .withColumn("discount_amount",col("after.Order_Discount_Amount"))
        .withColumn("net_amount",col("after.Order_Net_Amount"))
        .select("order_id_hash_key","load_timestamp","record_source","order_date","order_status","gross_amount","discount_amount","net_amount")
    )


# COMMAND ----------

@dlt.table(
    name = "sat_order_record_status"
)
def sat_order_record_status():
    return (
        dlt.readStream("order_raw_temp")
        .filter("order_id IS NOT NULL")
        .filter("Op_Type = 'c' OR Op_Type = 'd'")
        .withColumn("order_record_state",when(col("Op_Type") == lit('c'),"CREATED").otherwise("DELETED"))
        .select("order_id_hash_key","load_timestamp","record_source","order_record_state")
    )

# COMMAND ----------

@dlt.table(
    name = "link_customer_order"
)
def link_customer_order():
    return(
        dlt.readStream("order_raw_temp")
        .filter("order_id IS NOT NULL AND customer_id IS NOT NULL")
        .filter("Op_Type = 'c' ")
        .select("customer_id_order_id_hash_key","load_timestamp","record_source","customer_id_hash_key","order_id_hash_key")

    )
