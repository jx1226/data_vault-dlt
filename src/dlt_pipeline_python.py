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
def sat_customer_address_temp():
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

# COMMAND ----------

#Creating business vault table: Business vault is meant to apply soft business rules on the raw data vault. Transformations such as code-> description mappings can be performed in this layer.
@dlt.view(
    name = "sat_order_bv"
)
def sat_order_bv():
    return(
            dlt.read("sat_order")
            .withColumn("order_status",when(col("order_status") == lit('C'), lit("COMPLETED")) \
                .when(col("order_status") == lit('F'), lit("FAILED")) \
                    .when(col("order_status") == lit('R'), lit("REJECTED")).otherwise(lit("UNKNOWN")))
    )


# COMMAND ----------

#
@dlt.table(
    name = "dim_order"
)
def dim_order():
    hub_order = dlt.read("hub_order")
    sat_order = dlt.read("sat_order_bv")
    deleted_orders = dlt.read("sat_order_record_status").filter("order_record_state = 'DELETED'").select("order_id_hash_key","order_record_state")
    ret_df = hub_order.join(deleted_orders,["order_id_hash_key"],"left") \
    .filter("order_record_state is NULL") \
    .join(sat_order,["order_id_hash_key"],"inner") \
    .select(col("order_id_hash_key").alias("order_dim_key"),"order_date","order_status")
    return(
        ret_df
    )


# COMMAND ----------

@dlt.table(
    name = "dim_customer"
)
def dim_customer():
    hub_customer = dlt.read("hub_customer")
    sat_customer_personal = dlt.read("sat_customer_personal_detail") \
        .select("customer_id_hash_key",concat(col("first_name"), lit(" "), col("last_name")).alias("name"),"gender","dob")
    sat_customer_address = dlt.read("sat_customer_address").select("customer_id_hash_key", \
        col("load_timestamp").alias("cust_dim_start_timestamp"),"city","state","country",col("__END_AT").alias("customer_dim_end_timestamp"))
    
    ret_df = hub_customer.join(sat_customer_personal,["customer_id_hash_key"],"inner") \
    .join(sat_customer_address,["customer_id_hash_key"],"inner") \
    .select(sha1(concat("customer_id","cust_dim_start_timestamp")).alias("customer_dim_key"), \
        "customer_id","cust_dim_start_timestamp","name","gender","dob","city","state","country","customer_dim_end_timestamp")

    return(
        ret_df
    )

# COMMAND ----------

@dlt.table(
    name = "fact_order"
)
def fact_order():

    dlt.read("sat_customer_address").select("customer_id_hash_key",col("load_timestamp").alias("cust_addr_load_timestamp"),"__END_AT").na.fill(224969529600,["__END_AT"]).withColumn("customer_address_end_time",from_unixtime("__END_AT")).createOrReplaceTempView("customer_address")

    dlt.read("hub_customer").select("customer_id_hash_key","customer_id").createOrReplaceTempView("h_customer")

    customer_df = spark.sql("select hc.customer_id customer_id, hc.customer_id_hash_key customer_id_hash_key, ca.cust_addr_load_timestamp cust_addr_load_timestamp, ca.customer_address_end_time customer_address_end_time from h_customer hc JOIN customer_address ca on hc.customer_id_hash_key = ca.customer_id_hash_key")

    customer_df.createOrReplaceTempView("customers")

    hub_order = dlt.read("hub_order").select("order_id_hash_key","order_id")

    sat_order = dlt.read("sat_order_bv").select("order_id_hash_key",col("load_timestamp").alias("order_load_timestamp"),"order_date","order_status","gross_amount","discount_amount","net_amount").join(hub_order,["order_id_hash_key"],"inner")
    
    dlt.read("sat_order_record_status").filter("order_record_state = 'DELETED'").select("order_id_hash_key","order_record_state").createOrReplaceTempView("deleted_orders")

    dlt.read("link_customer_order").select("customer_id_order_id_hash_key","customer_id_hash_key","order_id_hash_key").join(sat_order,["order_id_hash_key"],"inner").createOrReplaceTempView("link")
     
    joined_df = spark.sql("select l.order_id_hash_key, l.order_id, l.gross_amount,l.discount_amount,l.net_amount, c.customer_id, c.cust_addr_load_timestamp from link l JOIN customers c ON l.customer_id_hash_key = c.customer_id_hash_key and l.order_load_timestamp >= c.cust_addr_load_timestamp and l.order_load_timestamp < c.customer_address_end_time LEFT JOIN deleted_orders do on l.order_id_hash_key = do.order_id_hash_key WHERE do.order_record_state IS NULL")

    ret_df = joined_df.select(sha1(concat(col("customer_id").cast(StringType()),col("order_id").cast(StringType()),"cust_addr_load_timestamp")).alias("fact_key"),col("order_id_hash_key").alias("order_dim_key"),sha1(concat(col("customer_id").cast(StringType()),"cust_addr_load_timestamp")).alias("customer_dim_key"),"discount_amount","net_amount","gross_amount")

    return(
        ret_df
    )
