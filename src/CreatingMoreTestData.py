# Databricks notebook source
# MAGIC %md
# MAGIC Objective is to demonstrate how Delta Live Table (DLT) maintains an Enterprise Data Warehouse(EDW) built on Data Vault methodology and how DLT can propagte all changes(Insertion/update/delete) happening at the source system to EDW(Data vault) and informational marts(dimensional models) layers.


# COMMAND ----------

# MAGIC %md
# MAGIC New load

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, LongType, FloatType, DoubleType, StructField
from pyspark.sql.functions import *
#import dbldatagen as dg
from pyspark.sql.functions import col



# COMMAND ----------

#updating customer address and adding a new customer
customer_data = [('xyz1000',(123456,'john','green','1983-05-09','M','apt#12','street 1','Stockholm','Stockholm','19345','Sweden','1234534','7458959392','john.green@gmail.com'),(123456,'john','green','1983-05-09','M','apt#121','street 1','Solna','Stockholm','19346','Sweden','1234534','7458959392','john.green@gmail.com'),'u',1714238911),('xyz1000',(123457,'don','yello','1983-01-08','M','apt#12','street 1','Sollentuna','Stockholm','19385','Sweden','1230534','7458949392','don.yellow@gmail.com'),(123457,'don','yello','1983-01-08','M','apt#123','street 1','Sigtuna','Stockholm','19388','Sweden','1230534','7458949392','don.yellow@gmail.com'),'u',1714238911),('xyz1000',None,(123458,'mag','svensson','1988-07-08','F','apt#112','street 11','Sodra','Stockholm','19385','Sweden','1230534','7458949392','mag.sven@gmail.com'),'c',1714238911)]


customer_schema = StructType([StructField('cdc_metada', StringType(), True), StructField('before', StructType([StructField('Customer_ID',IntegerType(),False),StructField('First_Name',StringType(),True),StructField('Last_Name',StringType(),True),StructField('DOB',StringType(),True),StructField('Gender',StringType(),True),StructField('Addr_Line1',StringType(),True),StructField('Addr_Line2',StringType(),True),StructField('city',StringType(),True),StructField('state',StringType(),True),StructField('ZipCode',StringType(),True),StructField('country',StringType(),True),StructField('Phone',StringType(),True),StructField('mobile',StringType(),True),StructField('email',StringType(),True)]), True), StructField('after', StructType([StructField('Customer_ID',IntegerType(),False),StructField('First_Name',StringType(),True),StructField('Last_Name',StringType(),True),StructField('DOB',StringType(),True),StructField('Gender',StringType(),True),StructField('Addr_Line1',StringType(),True),StructField('Addr_Line2',StringType(),True),StructField('city',StringType(),True),StructField('state',StringType(),True),StructField('ZipCode',StringType(),True),StructField('country',StringType(),True),StructField('Phone',StringType(),True),StructField('mobile',StringType(),True),StructField('email',StringType(),True)]), True),StructField('Op_Type',StringType(),True),StructField('Op_TimeStamp',LongType(),True)])

cust_df2 = spark.createDataFrame(customer_data,customer_schema)
cust_df2.write.mode("append").parquet("abfss://dlt@rawdata18042024.dfs.core.windows.net/retail/customer")

# COMMAND ----------

#adding more orders, to test incremental
order_data = [('xyz1111',None,(12341638,123456,'2023-03-27 19:39:22','C',700.0,100.0,600.0),'c',1714238911 ),('xyz1111',None,(12341639,123456,'2023-03-27 19:39:22','C',800.0,100.0,700.0),'c',1714238911),('xyz1111',None,(12341640,123457,'2023-03-27 19:39:22','C',500.0,50.0,450.0),'c',1714238911),('xyz1111',None,(12341641,123457,'2023-03-27 19:39:22','C',750.0,100.0,600.0),'c',1714238911),('xyz1111',None,(12341642,123458,'2023-03-27 19:39:22','C',750.0,100.0,600.0),'c',1714238911)]

order_schema = StructType([StructField('cdc_metada', StringType(), True), StructField('before', StructType([StructField('Order_Id',IntegerType(),False),StructField('Customer_ID',IntegerType(),False),StructField('Order_Date',StringType(),True),StructField('Order_Status',StringType(),True),StructField('Order_Gross_Amount',DoubleType(),True),StructField('Order_Discount_Amount',DoubleType(),True),StructField('Order_net_Amount',DoubleType(),True)]), True),StructField('after', StructType([StructField('Order_Id',IntegerType(),False),StructField('Customer_ID',IntegerType(),False),StructField('Order_Date',StringType(),True),StructField('Order_Status',StringType(),True),StructField('Order_Gross_Amount',DoubleType(),True),StructField('Order_Discount_Amount',DoubleType(),True),StructField('Order_net_Amount',DoubleType(),True)]), True),StructField('Op_Type',StringType(),True),StructField('Op_TimeStamp',LongType(),True)])

order_df2 = spark.createDataFrame(order_data,order_schema)
order_df2.write.mode("append").parquet("abfss://dlt@rawdata18042024.dfs.core.windows.net/retail/order")

# COMMAND ----------

#deleting one order 
order_data = [('xyz1111',(12341634,123456,'2023-03-26 15:36:02','C',700.0,100.0,600.0),None,'d',1714238911)]

order_schema = StructType([StructField('cdc_metada', StringType(), True), StructField('before', StructType([StructField('Order_Id',IntegerType(),False),StructField('Customer_ID',IntegerType(),False),StructField('Order_Date',StringType(),True),StructField('Order_Status',StringType(),True),StructField('Order_Gross_Amount',DoubleType(),True),StructField('Order_Discount_Amount',DoubleType(),True),StructField('Order_net_Amount',DoubleType(),True)]), True),StructField('after', StructType([StructField('Order_Id',IntegerType(),False),StructField('Customer_ID',IntegerType(),False),StructField('Order_Date',StringType(),True),StructField('Order_Status',StringType(),True),StructField('Order_Gross_Amount',DoubleType(),True),StructField('Order_Discount_Amount',DoubleType(),True),StructField('Order_net_Amount',DoubleType(),True)]), True),StructField('Op_Type',StringType(),True),StructField('Op_TimeStamp',LongType(),True)])

order_df3 = spark.createDataFrame(order_data,order_schema)
order_df3.write.mode("append").parquet("abfss://dlt@rawdata18042024.dfs.core.windows.net/retail/order")

# COMMAND ----------

#creating more Orders
order_data = [('xyz1111',None,(12341642,123456,'2023-04-03 17:58:02','C',700.0,100.0,600.0),'c',1714238911),('xyz1111',None,(12341643,123456,'2023-04-03 17:58:02','C',800.0,100.0,700.0),'c',1714238911),('xyz1111',None,(12341644,123457,'2023-04-03 17:58:02','C',500.0,50.0,450.0),'c',1714238911),('xyz1111',None,(12341645,123456,'2023-04-03 17:58:02','C',750.0,100.0,600.0),'c',1714238911)]

order_schema = StructType([StructField('cdc_metada', StringType(), True), StructField('before', StructType([StructField('Order_Id',IntegerType(),False),StructField('Customer_ID',IntegerType(),False),StructField('Order_Date',StringType(),True),StructField('Order_Status',StringType(),True),StructField('Order_Gross_Amount',DoubleType(),True),StructField('Order_Discount_Amount',DoubleType(),True),StructField('Order_net_Amount',DoubleType(),True)]), True),StructField('after', StructType([StructField('Order_Id',IntegerType(),False),StructField('Customer_ID',IntegerType(),False),StructField('Order_Date',StringType(),True),StructField('Order_Status',StringType(),True),StructField('Order_Gross_Amount',DoubleType(),True),StructField('Order_Discount_Amount',DoubleType(),True),StructField('Order_net_Amount',DoubleType(),True)]), True),StructField('Op_Type',StringType(),True),StructField('Op_TimeStamp',LongType(),True)])

order_df3 = spark.createDataFrame(order_data,order_schema)
