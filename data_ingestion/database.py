# Databricks notebook source
# DBTITLE 1,Import Packages
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from delta.tables import *
from datetime import date
from pyspark.sql.types import LongType, StringType, FloatType, DoubleType, DecimalType, StructType, StructField, IntegerType

import urllib

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,Create Database
# MAGIC %sql 
# MAGIC create database if not exists belvo_tables;

# COMMAND ----------

df=spark.sql('show databases')
df.show()

# COMMAND ----------

# DBTITLE 1,Read Files & Update
file_type = "csv"
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

schema_1 = StructType([
  StructField("company_Id", IntegerType(), True), 
  StructField("deal_Ids", StringType(), True), 
])

schema_2 = StructType([
  StructField("id", IntegerType(), True), 
  StructField("name", StringType(), True), 
  StructField("country", StringType(), True), 
])

schema_3 = StructType([
  StructField("contact_Id", IntegerType(), True), 
  StructField("deal_Ids", StringType(), True), 
])

schema_4 = StructType([
  StructField("ID", IntegerType(), True), 
  StructField("Owner_ID", IntegerType(), True),
  StructField("Customer_Name", StringType(), True),
  StructField("Customer_Phase", StringType(), True),
  StructField("Start_Date", StringType(), True),
  StructField("End_Date", StringType(), True),  
])

schema_5 = StructType([
  StructField("id", IntegerType(), True), 
  StructField("name", StringType(), True), 
  StructField("job", StringType(), True), 
  StructField("country", StringType(), True), 
  StructField("channel", StringType(), True), 
])

schema_6 = StructType([
  StructField("id", IntegerType(), True), 
  StructField("Name", StringType(), True),
  StructField("Team", StringType(), True),
  StructField("Job_position", StringType(), True),
])

schema_7 = StructType([
  StructField("id", IntegerType(), True), 
  StructField("externalId", StringType(), True), 
  StructField("ownerId", StringType(), True), 
  StructField("name", StringType(), True), 
  StructField("product", StringType(), True), 
  StructField("amount", StringType(), True),
  StructField("closed", StringType(), True), 
  StructField("status", StringType(), True), 
  StructField("created_date", StringType(), True), 
  StructField("closed_2", StringType(), True), 
])

df1 = spark.read.format(file_type).schema(schema_1).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load("dbfs:/FileStore/shared_uploads/juliacmoura97@gmail.com/companies_deals_associations.csv")
df2 = spark.read.format(file_type).schema(schema_2).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load("dbfs:/FileStore/shared_uploads/juliacmoura97@gmail.com/companies.csv")
df3 = spark.read.format(file_type).schema(schema_3).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load("dbfs:/FileStore/shared_uploads/juliacmoura97@gmail.com/contacts_deals_associations.csv")
df4 = spark.read.format(file_type).schema(schema_4).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load("dbfs:/FileStore/shared_uploads/juliacmoura97@gmail.com/customers.csv")
df5 = spark.read.format(file_type).schema(schema_5).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load("dbfs:/FileStore/shared_uploads/juliacmoura97@gmail.com/contacts.csv")
df6 = spark.read.format(file_type).schema(schema_6).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load("dbfs:/FileStore/shared_uploads/juliacmoura97@gmail.com/owners-1.csv")
df7 = spark.read.format(file_type).schema(schema_7).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load("dbfs:/FileStore/shared_uploads/juliacmoura97@gmail.com/deals.csv")


# COMMAND ----------

# DBTITLE 1,Select Database
# MAGIC %sql
# MAGIC -- Check the current database
# MAGIC SELECT CURRENT_DATABASE();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change the current database
# MAGIC USE belvo_tables;
# MAGIC -- Check the current database
# MAGIC SELECT CURRENT_DATABASE();

# COMMAND ----------

# DBTITLE 1,Create Tables
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

df1.write.format("delta").mode("overwrite").saveAsTable("belvo_tables.companies_deals_associations")
df2.write.format("delta").mode("overwrite").saveAsTable("belvo_tables.companies")
df3.write.format("delta").mode("overwrite").saveAsTable("belvo_tables.contacts_deals_associations")
df4.write.format("delta").mode("overwrite").saveAsTable("belvo_tables.customers")
df5.write.format("delta").mode("overwrite").saveAsTable("belvo_tables.contacts")
df6.write.format("delta").mode("overwrite").saveAsTable("belvo_tables.owners")
df7.write.format("delta").mode("overwrite").saveAsTable("belvo_tables.deals")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CHeck tables in a database
# MAGIC SHOW TABLES IN belvo_tables
