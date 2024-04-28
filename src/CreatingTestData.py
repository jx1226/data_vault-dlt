# Databricks notebook source
# MAGIC %md
# MAGIC Objective is to demonstrate how Delta Live Table (DLT) maintains an Enterprise Data Warehouse(EDW) built on Data Vault methodology and how DLT can propagte all changes(Insertion/update/delete) happening at the source system to EDW(Data vault) and informational marts(dimensional models) layers.

# COMMAND ----------

# MAGIC %md
# MAGIC Initial load

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, LongType, FloatType, DoubleType, StructField
from pyspark.sql.functions import *
#import dbldatagen as dg
from pyspark.sql.functions import col

# COMMAND ----------

#Creating new customers 
#Customer Payload:- {cdc_metada(), before (CustomerID, FirstName, LastName, DOB, AddrLine1, AddrLine2, City, State, ZipCode, Country, Phone, Mobile, Email),after (CustomerID, FirstName, LastName, DOB, AddrLine1, AddrLine2, City, County, ZipCode, Country, Phone, Mobile, Email, Op_Type), Op_Type, Op_TimeStamp }

customer_data = [('xyz1000',None,(123456,'john','green','1983-05-09','M','apt#12','street 1','Stockholm','Stockholm','19345','Sweden','1234534','7458959392','john.green@gmail.com'),'c',1679844962),('xyz1000',None,(123457,'don','yello','1983-01-08','M','apt#12','street 1','Sollentuna','Stockholm','19385','Sweden','1230534','7458949392','don.yellow@gmail.com'),'c',1679844962)]

customer_schema = StructType([StructField('cdc_metada', StringType(), True), StructField('before', StructType([StructField('Customer_ID',IntegerType(),False),StructField('First_Name',StringType(),True),StructField('Last_Name',StringType(),True),StructField('DOB',StringType(),True) ,StructField('Gender',StringType(),True),StructField('Addr_Line1',StringType(),True),StructField('Addr_Line2',StringType(),True),StructField('city',StringType(),True),StructField('state',StringType(),True),StructField('ZipCode',StringType(),True),StructField('country',StringType(),True),StructField('Phone',StringType(),True),StructField('mobile',StringType(),True),StructField('email',StringType(),True)]), True), StructField('after', StructType([StructField('Customer_ID',IntegerType(),False),StructField('First_Name',StringType(),True),StructField('Last_Name',StringType(),True),StructField('DOB',StringType(),True),StructField('Gender',StringType(),True),StructField('Addr_Line1',StringType(),True),StructField('Addr_Line2',StringType(),True),StructField('city',StringType(),True),StructField('state',StringType(),True),StructField('ZipCode',StringType(),True),StructField('country',StringType(),True),StructField('Phone',StringType(),True),StructField('mobile',StringType(),True),StructField('email',StringType(),True)]), True),StructField('Op_Type',StringType(),True),StructField('Op_TimeStamp',LongType(),True)])

cust_df1 = spark.createDataFrame(customer_data,customer_schema)

cust_df1.write.mode("overwrite").parquet("abfss://dlt@rawdata18042024.dfs.core.windows.net/retail/customer")


# COMMAND ----------

display(cust_df1)

# COMMAND ----------

#Creating new Orders
#OrderPayload -  (cdc_metada(), before (order_id,customer_id,order_date,order_status,order_gross_amount,order_discount_amount,order_net_amount), after(order_id,customer_id,order_date,order_status,order_gross_amount,order_discount_amount,order_net_amount), Op_Type, Op_TimeStamp )
#Creating new Order data
order_data = [('xyz1111',None,(12341634,123456,'2023-03-26 15:36:02','C',700.0,100.0,600.0),'c',1679844962),('xyz1111',None,(12341635,123456,'2023-03-26 15:36:05','C',800.0,100.0,700.0),'c',1679844962),('xyz1111',None,(12341636,123457,'2023-03-26 15:36:08','C',500.0,50.0,450.0),'c',1679844962),('xyz1111',None,(12341637,123457,'2023-03-26 15:36:09','C',750.0,100.0,650.0),'c',1679844962)]

order_schema = StructType([StructField('cdc_metada', StringType(), True), StructField('before', StructType([StructField('Order_Id',IntegerType(),False),StructField('Customer_ID',IntegerType(),False),StructField('Order_Date',StringType(),True),StructField('Order_Status',StringType(),True),StructField('Order_Gross_Amount',DoubleType(),True),StructField('Order_Discount_Amount',DoubleType(),True),StructField('Order_net_Amount',DoubleType(),True)]), True),StructField('after', StructType([StructField('Order_Id',IntegerType(),False),StructField('Customer_ID',IntegerType(),False),StructField('Order_Date',StringType(),True),StructField('Order_Status',StringType(),True),StructField('Order_Gross_Amount',DoubleType(),True),StructField('Order_Discount_Amount',DoubleType(),True),StructField('Order_net_Amount',DoubleType(),True)]), True),StructField('Op_Type',StringType(),True),StructField('Op_TimeStamp',LongType(),True)])

order_df1 = spark.createDataFrame(order_data,order_schema)
order_df1.write.mode("overwrite").parquet("abfss://dlt@rawdata18042024.dfs.core.windows.net/retail/order")

