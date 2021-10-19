# Databricks notebook source
# MAGIC %md This notebook was developed for a moderately sized, *i.e.* more than one-worker node, **Databricks 8.3 cluster** on which the **com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18** Maven package has been installed.  For more details on the installation of Maven packages, please refer to [this document](https://docs.databricks.com/libraries/cluster-libraries.html). 
# MAGIC 
# MAGIC The purpose of this notebook is to process inventory change event and snapshot data being transmitted into the Azure infrastructure from the various (simulated) point-of-sale systems in this demonstration.  As they are received, these data are landed into various Delta tables, enabling persistence and downstream stream processing.
# MAGIC 
# MAGIC This notebook should run while the *POS 02* notebook (which generates the simulated event data) runs on a separate cluster. It also depends on the demo environment having been configured per the instructions in the *POS 01* notebook.
# MAGIC 
# MAGIC **NOTE** The stream processing commands in this notebook will run indefinitely unless explicitly stopped or the executing cluster is terminated.

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
import pyspark.sql.functions as f
from pyspark.sql.types import *

from delta.tables import *

import time

# COMMAND ----------

# DBTITLE 1,Notebook Configuration
# MAGIC %run "./POS 01: Environment Setup"

# COMMAND ----------

# MAGIC %md ## Step 1: Setup the POS Database Environment
# MAGIC 
# MAGIC In this notebook, we will land multiple types of data into various Delta tables housed within a database named *pos*. To ensure a clean environment, we will recreate this database now and delete any checkpoints associated with streaming ETL routines populating the tables within it:
# MAGIC 
# MAGIC **NOTE** The Azure IOT Hub is not reset between runs of this notebook and the data generation logic in *POS 02* starts at the beginning of the simulated dataset with each restart. To ensure a clean environment between runs, you may wish to first delete your Azure IOT Hub deployment and then recreate it.  This will require you to repeat the configuration steps outlines in the *POS 01* notebook and update the appropriate configuration values within it.

# COMMAND ----------

# DBTITLE 1,Setup Database
# MAGIC %sql
# MAGIC 
# MAGIC drop database if exists pos cascade;
# MAGIC create database pos;

# COMMAND ----------

# DBTITLE 1,Delete Old Checkpoints
dbutils.fs.rm(config['inventory_change_checkpoint_path'], recurse=True)
dbutils.fs.rm(config['inventory_snapshot_checkpoint_path'], recurse=True)
dbutils.fs.rm(config['inventory_current_checkpoint_path'], recurse=True)

# COMMAND ----------

# MAGIC %md ## Step 2: Load the Static Reference Data
# MAGIC 
# MAGIC While we've given attention in this and other notebooks to the fact that we are receiving streaming event data and periodic snapshots, we also have reference data we need to access.  These data are relatively stable and would be updated using a classic, scheduled batch ETL process.  
# MAGIC 
# MAGIC To represent this kind of data, we will perform a one-time load of store, item and inventory change type reference data:

# COMMAND ----------

# DBTITLE 1,Stores
store_schema = StructType([
  StructField('store_id', IntegerType()),
  StructField('name', StringType())
  ])





# COMMAND ----------

store_schema

# COMMAND ----------

(
  spark
    .read
    .csv(config['stores_filename'], header=True, schema=store_schema)
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema','true')
    .saveAsTable('pos.store')
  )

# COMMAND ----------

#config['stores_filename']

! ls /dbfs//mnt/pos/static_data/store.txt

# COMMAND ----------

display(spark.table('pos.store'))

# COMMAND ----------

# DBTITLE 1,Items
item_schema = StructType([
  StructField('item_id', IntegerType()),
  StructField('name', StringType()),
  StructField('supplier_id', IntegerType()),
  StructField('safety_stock_quantity', IntegerType())
  ])

(
  spark
    .read
    .csv(config['items_filename'], header=True, schema=item_schema).write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema','true')
    .saveAsTable('pos.item')
  )

display(spark.table('pos.item'))

# COMMAND ----------

# DBTITLE 1,Inventory Change Types
change_type_schema = StructType([
  StructField('change_type_id', IntegerType()),
  StructField('change_type', StringType())
  ])

(
  spark
    .read
    .csv(config['change_types_filename'], header=True, schema=change_type_schema)
    .write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema','true')
    .saveAsTable('pos.inventory_change_type')
  )

display(spark.table('pos.inventory_change_type'))

# COMMAND ----------

# MAGIC %md ## Step 3: Stream Inventory Change Events
# MAGIC 
# MAGIC Let's now tackle our inventory change event data. These data consist of a JSON document transmitted by a store to summarize an event with inventory relevance. These events may represent sales, restocks, or reported loss, damage or theft (shrinkage).  A fourth event type, *bopis*, indicates a sales transaction that takes place in the online store but which is fulfilled by a physical store. (The *snapshot* inventory change type visible in the results of the prior step will be addressed later.)  All these events are transmitted as part of a consolidated stream:
# MAGIC 
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/pos_event_change_streaming_etl.png' width=600>
# MAGIC 
# MAGIC As these events arrive in the Azure IOT Hub from the store, they are read in batches of 100 event messages per executor with each streaming cycle as indicated by the Event Hub Connector's [maxEventsPerTrigger](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md) configuration setting. The use of throttling in this manner ensures the limited resources provisioned for the streaming ETL do not get overwhelmed by sudden bursts in incoming data:

# COMMAND ----------

# DBTITLE 1,Read Event Stream
# connection information
eventhub_config = {}
eventhub_config['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(config['event_hub_connection_string'])
eventhub_config['maxEventsPerTrigger'] = (100 * sc.defaultParallelism) # throttle stream to limit resource requirements

raw_inventory_change = (
  spark
    .readStream
    .format('eventhubs')
    .options(**eventhub_config)
    .load()
    .withColumn('body', f.expr('cast(body as string)')) # convert binary body to string
  )

display(raw_inventory_change)

# COMMAND ----------

# MAGIC %md Each event read from the IOT Hub forms a record in the streaming DataFrame. The body field represents the message transmitted from a store to our environment, and by casting it as a string, we can see the JSON structure of that data.
# MAGIC 
# MAGIC To access the data in that string, we can apply the [from_json()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.from_json.html) function, providing details of the anticipated structure of the JSON document. Using the [explode()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.explode.html) function, we can extract the elements in the items array, marrying them with the event details with which they were transmitted.  This leaves us with a data set where each record represents an item involved in an inventory-relevant transaction.
# MAGIC 
# MAGIC While we do not anticipate duplicate event messages to be transmitted from our source POS environments, we may include functionality here to deal with the possibility that duplicate records may be transmitted for a variety of reasons outside of our control.  Using the [dropDuplicates()](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#streaming-deduplication) function and keying off the globally-unique transaction ID and associated item IDs as the unique identifiers for any record flowing through the stream, we can inspect records looking for duplicates.  In order to keep memory pressure on the stream manageable, we are setting a [watermark](https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html) of 1-hour. This means that the stream will hold onto the transaction IDs and item IDs for all records crossing through it from the maximum observe (current) date time associated with an event message to 1-hour prior, enabling duplicate detection across that set of records.  Any record arriving outside that window will be discarded:

# COMMAND ----------

# DBTITLE 1,Convert Transaction to Structure Field
transaction_schema = StructType([
  StructField('trans_id', StringType()),
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('change_type_id', IntegerType()),
  StructField('items', ArrayType(
    StructType([
      StructField('item_id', IntegerType()), 
      StructField('quantity', IntegerType())
      ])
    ))
  ])

parsed_inventory_change = (
  raw_inventory_change
    .withColumn('event', f.from_json('body', transaction_schema)) # parse data in payload
    .select(
      f.col('event.trans_id').alias('trans_id'), # extract relevant fields from payload
      f.col('event.store_id').alias('store_id'), 
      f.col('event.date_time').alias('date_time'), 
      f.col('event.change_type_id').alias('change_type_id'), 
      f.explode('event.items').alias('item')     # explode items so that there is now one item per record
      )
    .withColumn('item_id', f.col('item.item_id'))
    .withColumn('quantity', f.col('item.quantity'))
    .drop('item')
    .withWatermark('date_time', '1 hour') 
    .dropDuplicates(['trans_id','item_id'])  # drop duplicates 
  )

display(parsed_inventory_change)

# COMMAND ----------

# MAGIC %md The transformed data can now be streamed to a Delta table for long-term persistence.  We chose the *append* output mode as the change data should always represent an incremental change in inventory making a simple insertion of incoming records to the table a logical choice.  
# MAGIC 
# MAGIC To ensure this flow of data can recover from an interruption, a checkpoint location is set as described [here](https://docs.databricks.com/spark/latest/structured-streaming/production.html):

# COMMAND ----------

# DBTITLE 1,Persist Inventory Change Data
_ = (
  parsed_inventory_change
    .writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation',config['inventory_change_checkpoint_path'])
    .table('pos.inventory_change')
    )

# COMMAND ----------

# MAGIC %md So, why exactly are we using the Delta format for the *pos.inventory_change* table? Because this data is arriving on a near-continuous basis, the [ACID compliant features](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html) of Delta ensure that downstream queries against this table will return data in a consistent state. We expect to accumulate a large volume of data over time and the [indexing and file management](https://docs.databricks.com/delta/optimizations/file-mgmt.html) capabilities of Delta ensure it will also perform well as it scales. 
# MAGIC 
# MAGIC But most importantly for our needs, Delta has the ability to [serve as both](https://docs.delta.io/latest/delta-streaming.html) a streaming sink (as we are using it here) and a streaming source (as we will use it in the next notebook).  This means that other routines can stream from the *pos.inventory_change* table and as new records land through the ETL defined here, they are made immediately available to those downstream processes.  In this way, Delta enables a convenient hand-off point between our more technically-oriented initial ETL routine and any downstream ETL routines which may be more business-oriented. For more information about this *Delta pattern*, please check out [this blog](https://databricks.com/blog/2020/11/23/acid-transactions-on-data-lakes.html).

# COMMAND ----------

# MAGIC %md ##Step 4: Stream Inventory Snapshots
# MAGIC 
# MAGIC Periodically, we receive counts of items in inventory at a given store location.  Such inventory snapshots are frequently used by retailers to update their understanding of which products are actually on-hand given concerns about the reliability of calculated inventory quantities.
# MAGIC 
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/pos_snapshot_auto_loader_etl.png' width=600>
# MAGIC 
# MAGIC These inventory snapshot data arrive in this environment as CSV files on a slightly irregular basis. But as soon as they land, we will want to process them, making them available to support more accurate estimates of current inventory. To enable this, we will take advantage of the Databricks [Auto Loader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) feature which listens for incoming files to a storage path and processes the data for any newly arriving files as a stream:

# COMMAND ----------

# DBTITLE 1,Access Incoming Snapshots
inventory_snapshot_schema = StructType([
  StructField('id', IntegerType()),
  StructField('item_id', IntegerType()),
  StructField('employee_id', IntegerType()),
  StructField('store_id', IntegerType()),
  StructField('date_time', TimestampType()),
  StructField('quantity', IntegerType())
  ])

raw_inventory_snapshot = (
  spark
    .readStream
    .format('cloudFiles')  # auto loader
    .option('cloudFiles.format', 'csv')
    .option('cloudFiles.includeExistingFiles', 'true') 
    .option('header', 'true')
    .schema(inventory_snapshot_schema)
    .load(config['inventory_snapshot_path'])
    .drop('id')
  )

display(raw_inventory_snapshot)

# COMMAND ----------

# MAGIC %md With Auto Loader providing access to the incoming snapshot data as a stream, we can persist it much like we did before to Delta table.  Here we are simply appending all incoming snapshot data to *pos.inventory_snapshot* so that we might have full access to our snapshot history:

# COMMAND ----------

# DBTITLE 1,Persist Snapshots
_ = (
  raw_inventory_snapshot
    .writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', config['inventory_snapshot_checkpoint_path'])
    .table('pos.inventory_snapshot')
    )

# COMMAND ----------

# MAGIC %md While a complete history is great for some analyses, for others it may be helpful to have just the latest snapshot count for a product in a given location. To provide fast access to this kind of data, we will setup a table named *latest_inventory_snapshot* within which a single record will represent the latest snapshot count of a product within a store.  As new data arrives, we'll need to perform a [merge](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge) on the table, updating existing product-store records and appending any new ones. 
# MAGIC 
# MAGIC To enable this, we need to initialize our target table:

# COMMAND ----------

# DBTITLE 1,Create Latest Snapshots Table
# MAGIC %sql
# MAGIC 
# MAGIC create table if not exists pos.latest_inventory_snapshot (
# MAGIC   item_id int,
# MAGIC   employee_id int,
# MAGIC   store_id int,
# MAGIC   date_time timestamp,
# MAGIC   quantity int
# MAGIC   )
# MAGIC   using delta;

# COMMAND ----------

# MAGIC %md Next, we'll need to write a routine to provide the data processing logic for each microbatch of data that flows through our snapshot ETL pipeline.  In the code below, this logic captured in the custom *upsertToDelta* function.  This function receives the records flowing through a single microbatch cycle as a Spark DataFrame.  From there, we can specify standard data modification logic (DML) as needed to achieve our goals:

# COMMAND ----------

# DBTITLE 1,Define Custom Inventory Snapshot DML Logic
# identify snapshot table
snapshot_target = DeltaTable.forName(spark, 'pos.latest_inventory_snapshot')

# function to merge batch data with delta table
def upsertToDelta(microBatchOutputDF, batchId):
  
  # merge changes with target table 
  ( 
    snapshot_target.alias('t')
      .merge(
        microBatchOutputDF.alias('s'),
        's.store_id=t.store_id AND s.item_id=t.item_id AND s.date_time<=t.date_time'
        )   # match on store_id and item_id and when source rec is prior to incoming rec
    .whenMatchedUpdateAll() 
    .whenNotMatchedInsertAll()
    .execute()
    )
  
  # insert dummy records to ensure matches for inner joins with change data
  ( 
    microBatchOutputDF
      .selectExpr(
        'NULL as trans_id', 
        'store_id', 
        'date_time',
        '-1 as change_type_id',
        'item_id',
        '0 as quantity'
        )
      .write
      .format('delta')
      .mode('append')
      .saveAsTable('pos.inventory_change')
    )

# COMMAND ----------

# MAGIC %md Notice the *upsertToDelta* function does more than simply merge (update+insert) our data with the *pos.latest_inventory_snapshot* table.  It provides additional logic to create *dummy records* for each item flowing through our snapshot pipeline and then inserts these into the *pos.inventory_change* table (populated above through our change event ETL).  These dummy records solve a problem we will discuss in the next notebook, but for now just note that we have incredible flexibility with our data processing through this function.
# MAGIC 
# MAGIC With our data modification function defined, we now apply it to the streaming snapshot data using the [foreachBatch()](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch) method.  This method allows us to apply a function to each cycle of data, *i.e.* microbatch, that moves through our streaming pipeline.  Applying our *upsertToDelta* function, we can now populate our *pos.latest_inventory_change* table as needed:

# COMMAND ----------

# DBTITLE 1,Apply Custom DML to Inventory Snapshot Data
# start the query to continuously upsert into aggregates tables in update mode
_ = (
  raw_inventory_snapshot
    .writeStream
    .format('delta')
    .foreachBatch(upsertToDelta)
    .outputMode('update')
    .start()
  )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | azure-iot-device                                     | Microsoft Azure IoT Device Library | MIT    | https://pypi.org/project/azure-iot-device/                       |
# MAGIC | azure-storage-blob                                | Microsoft Azure Blob Storage Client Library for Python| MIT        | https://pypi.org/project/azure-storage-blob/      |
# MAGIC | com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18 | Azure EventHubs Connector for Apache Spark (Spark Core, Spark Streaming, Structured Streaming) | Apache | https://search.maven.org/artifact/com.microsoft.azure/azure-eventhubs-spark_2.12/2.3.18/jar |
