# Databricks notebook source
# MAGIC %md The purpose of this notebook is to prepare the dataset we will use to build a deep & wide collaborative filter recommender.  This notebook should be run on a **Databricks 8.1+ ML cluster**. 

# COMMAND ----------

# MAGIC %md # Introduction 
# MAGIC 
# MAGIC Collaborative filters leverage similarities between users to make recommendations:
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/images/instacart_collabrecom.png" width="600">
# MAGIC 
# MAGIC Unlike with memory-based collaborative filters which employ the weighted averaging of product ratings (explicit or implied) between similar users, model-based collaborative filters leverage the features associated with user-product combinations to predict that a given user will click-on or purchase a particular item.  To build such a model, we will need information about users and the products they have purchased.

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql import window as w 

# COMMAND ----------

# MAGIC %md # Step 1: Load the Data
# MAGIC 
# MAGIC The basic building block of the collaborative filter is transactional data containing a customer identifier. The popular [Instacart dataset](https://www.kaggle.com/c/instacart-market-basket-analysis) provides us a nice collection of such data with over 3 million grocery orders placed by over 200,000 Instacart users over a nearly 2-year period across of portfolio of nearly 50,000 products. This is the same dataset used in the construction of a memory-based collaborative filter as documented in a [previously published set of notebooks](https://databricks.com/blog/2020/12/18/personalizing-the-customer-experience-with-recommendations.html) which provides a nice comparison to the techniques explored here.
# MAGIC 
# MAGIC **NOTE** Due to the terms and conditions by which these data are made available, anyone interested in recreating this work will need to download the data files from Kaggle and upload them to a folder structure as described below.
# MAGIC 
# MAGIC The primary data files available for download are organized as follows under a pre-defined [mount point](https://docs.databricks.com/data/databricks-file-system.html#mount-object-storage-to-dbfs) that we have named */mnt/instacart*:
# MAGIC 
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/instacart_filedownloads.png' width=250>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Read into dataframes, these files form the following data model which captures the products customers have included in individual transactions:
# MAGIC 
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/instacart_schema2.png' width=300>
# MAGIC 
# MAGIC We will apply minimal transformations to this data, persisting it to the Delta Lake format for speedier access:

# COMMAND ----------

if 'config' not in locals():
  config = {}
config['storage_account_name'] = 'posanalysis'
config['storage_container_name'] = 'instacart'
config['storage_account_access_key'] = '1KoGkMGScnFqeqoJJDQ9HgUBcLHel798LtR6e3yy/xDugn8Jdkme0WCGiI5n6+vqfYsDE7Wea5qqVOInlkVInw=='
config['storage_connection_string'] = 'DefaultEndpointsProtocol=https;AccountName=posanalysis;AccountKey=1KoGkMGScnFqeqoJJDQ9HgUBcLHel798LtR6e3yy/xDugn8Jdkme0WCGiI5n6+vqfYsDE7Wea5qqVOInlkVInw==;EndpointSuffix=core.windows.net'

# COMMAND ----------

config['dbfs_mount_name'] = '/mnt/instacart'
conf_key_name = "fs.azure.account.key.{0}.blob.core.windows.net".format(config['storage_account_name'])
conf_key_value = config['storage_account_access_key']

# determine if not already mounted
for m in dbutils.fs.mounts():
  mount_exists = (m.mountPoint==config['dbfs_mount_name'])
  if mount_exists: break

# create mount if not exists
if not mount_exists:
  
  print('creating mount point {0}'.format(config['dbfs_mount_name']))
  
  # create mount
  dbutils.fs.mount(
    source = "wasbs://{0}@{1}.blob.core.windows.net".format(
      config['storage_container_name'], 
      config['storage_account_name']
      ),
    mount_point = config['dbfs_mount_name'],
    extra_configs = {conf_key_name:conf_key_value}
    )

# COMMAND ----------

!ls /dbfs/mnt/instacart/bronze/order_products/

# COMMAND ----------

# DBTITLE 1,Create Database
_ = spark.sql('CREATE DATABASE IF NOT EXISTS instacart')

# COMMAND ----------

# MAGIC %md The orders data is pre-divided into *prior* and *training* evaluation sets, where the *training* dataset represents the last order placed in the overall sequence of orders associated with a given customer.  The *prior* dataset represents those orders that proceed the *training* order.  In a previous set of notebooks built on this data, we relabeled the *prior* and *training* evaluation sets as *calibration* and *evaluation*, respectively, to better align terminology with how the data was being used.  Here, we will preserve the *prior* & *training* designations as this better aligns with our current modeling needs.
# MAGIC 
# MAGIC We will add to this dataset a field, *days_prior_to_last_order*, which calculates the days from a given order to the order that represents the *training* instance. This field will help us when developing features around purchases taking place different intervals prior to the final order.  All other tables will be brought into the database without schema changes, simply converting the underlying format from CSV to delta lake for better query performance later:

# COMMAND ----------

# MAGIC %sh
# MAGIC for file in `find /dbfs/mnt/instacart -name "*.zip" -type f`
# MAGIC do
# MAGIC filename=`basename $file`
# MAGIC mv $file  /tmp/$filename
# MAGIC unzip /tmp/$filename
# MAGIC target_dir=`dirname $file`
# MAGIC target_file=`basename -s '.zip' $file`
# MAGIC rm -f $file
# MAGIC 
# MAGIC mv /tmp/$target_file  $target_dir/$target_file
# MAGIC done

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC 
# MAGIC mv /tmp/aisles.csv.zip  ~/.
# MAGIC unzip ~/aisles.csv.zip

# COMMAND ----------

# DBTITLE 1,Load Orders
# delete the old table if needed
_ = spark.sql('DROP TABLE IF EXISTS instacart.orders')

# define schema for incoming data
orders_schema = StructType([
  StructField('order_id', IntegerType()),
  StructField('user_id', IntegerType()),
  StructField('eval_set', StringType()),
  StructField('order_number', IntegerType()),
  StructField('order_dow', IntegerType()),
  StructField('order_hour_of_day', IntegerType()),
  StructField('days_since_prior_order', FloatType())
  ])

# read data from csv
orders = (
  spark
    .read
    .csv(
      '/mnt/instacart/bronze/orders',
      header=True,
      schema=orders_schema,
      
      )
  )

# COMMAND ----------

display(orders)

# COMMAND ----------

# DBTITLE 1,Orders


# calculate days until final purchase 
win = (
  w.Window.partitionBy('user_id').orderBy(f.col('order_number').desc())
  )

orders_enhanced = (
    orders
      .withColumn(
        'days_prior_to_last_order', 
        f.sum('days_since_prior_order').over(win) - f.coalesce(f.col('days_since_prior_order'),f.lit(0))
        ) 
  )

# write data to delta
(
  orders_enhanced
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema','true')
    .save('/mnt/instacart/silver/orders')
  )

# make accessible as spark sql table
_ = spark.sql('''
  CREATE TABLE instacart.orders
  USING DELTA
  LOCATION '/mnt/instacart/silver/orders'
  ''')

# present the data for review
display(
  spark
    .table('instacart.orders')
    .orderBy('user_id','order_number')
  )

# COMMAND ----------

display(spark.table("instacart.orders").where("user_id == 1"))

# COMMAND ----------

# DBTITLE 1,Products
# delete the old table if needed
_ = spark.sql('DROP TABLE IF EXISTS instacart.products')

# define schema for incoming data
products_schema = StructType([
  StructField('product_id', IntegerType()),
  StructField('product_name', StringType()),
  StructField('aisle_id', IntegerType()),
  StructField('department_id', IntegerType())
  ])

# read data from csv
products = (
  spark
    .read
    .csv(
      '/mnt/instacart/bronze/products',
      header=True,
      schema=products_schema
      )
  )

# write data to delta
(
  products
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema','true')
    .save('/mnt/instacart/silver/products')
  )

# make accessible as spark sql table
_ = spark.sql('''
  CREATE TABLE instacart.products
  USING DELTA
  LOCATION '/mnt/instacart/silver/products'
  ''')

# present the data for review
display(
  spark.table('instacart.products')
  )

# COMMAND ----------

# DBTITLE 1,Order Products
# delete the old table if needed
_ = spark.sql('DROP TABLE IF EXISTS instacart.order_products')

# define schema for incoming data
order_products_schema = StructType([
  StructField('order_id', IntegerType()),
  StructField('product_id', IntegerType()),
  StructField('add_to_cart_order', IntegerType()),
  StructField('reordered', IntegerType())
  ])

# read data from csv
order_products = (
  spark
    .read
    .csv(
      '/mnt/instacart/bronze/order_products',
      header=True,
      schema=order_products_schema
      )
  )

# write data to delta
(
  order_products
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema','true')
    .save('/mnt/instacart/silver/order_products')
  )

# make accessible as spark sql table
_ = spark.sql('''
  CREATE TABLE instacart.order_products
  USING DELTA
  LOCATION '/mnt/instacart/silver/order_products'
  ''')

# present the data for review
display(
  spark.table('instacart.order_products')
  )

# COMMAND ----------

# DBTITLE 1,Departments
# delete the old table if needed
_ = spark.sql('DROP TABLE IF EXISTS instacart.departments')

# define schema for incoming data
departments_schema = StructType([
  StructField('department_id', IntegerType()),
  StructField('department', StringType())  
  ])

# read data from csv
departments = (
  spark
    .read
    .csv(
      '/mnt/instacart/bronze/departments',
      header=True,
      schema=departments_schema
      )
  )

# write data to delta
(
  departments
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema','true')
    .save('/mnt/instacart/silver/departments')
  )

# make accessible as spark sql table
_ = spark.sql('''
  CREATE TABLE instacart.departments
  USING DELTA
  LOCATION '/mnt/instacart/silver/departments'
  ''')

# present the data for review
display(
  spark.table('instacart.departments')
  )

# COMMAND ----------

# DBTITLE 1,Aisles
# delete the old table if needed
_ = spark.sql('DROP TABLE IF EXISTS instacart.aisles')

# define schema for incoming data
aisles_schema = StructType([
  StructField('aisle_id', IntegerType()),
  StructField('aisle', StringType())  
  ])

# read data from csv
aisles = (
  spark
    .read
    .csv(
      '/mnt/instacart/bronze/aisles',
      header=True,
      schema=aisles_schema
      )
  )

# write data to delta
(
  aisles
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema','true')
    .save('/mnt/instacart/silver/aisles')
  )

# make accessible as spark sql table
_ = spark.sql('''
  CREATE TABLE instacart.aisles
  USING DELTA
  LOCATION '/mnt/instacart/silver/aisles'
  ''')

# present the data for review
display(
  spark.table('instacart.aisles')
  )

# COMMAND ----------

# MAGIC %md # Step 2: Combine Order Details
# MAGIC 
# MAGIC With our data loaded, we will flatten our order details through a view.  This will make access to our data during feature engineering significantly easier:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS instacart.order_details;
# MAGIC 
# MAGIC CREATE VIEW instacart.order_details as
# MAGIC   SELECT
# MAGIC     a.eval_set,
# MAGIC     a.user_id,
# MAGIC     a.order_number,
# MAGIC     a.order_id,
# MAGIC     a.order_dow,
# MAGIC     a.order_hour_of_day,
# MAGIC     a.days_since_prior_order,
# MAGIC     a.days_prior_to_last_order,
# MAGIC     b.product_id,
# MAGIC     c.aisle_id,
# MAGIC     c.department_id,
# MAGIC     b.reordered
# MAGIC   FROM instacart.orders a
# MAGIC   INNER JOIN instacart.order_products b
# MAGIC     ON a.order_id=b.order_id
# MAGIC   INNER JOIN instacart.products c
# MAGIC     ON b.product_id=c.product_id;
# MAGIC     
# MAGIC SELECT *
# MAGIC FROM instacart.order_details;

# COMMAND ----------

# MAGIC %md © 2021 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License. All included or referenced third party libraries are subject to the licenses set forth below.
