# Databricks notebook source
# MAGIC %md The purpose of this notebook is to demonstrate how current inventory may be calculated using incoming point-of-sale data. It was developed for a moderately sized, *i.e.* more than one-worker node, **Databricks 8.3 cluster**.  This may be the same cluster as is used for running the *POS 03* notebook which should be running in parallel with this one.
# MAGIC 
# MAGIC **NOTE** The stream processing commands in this notebook will run indefinitely unless explicitly stopped or the executing cluster is terminated.

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
import pyspark.sql.functions as f

# COMMAND ----------

# DBTITLE 1,Notebook Configuration
# MAGIC %run "./POS 01: Environment Setup"

# COMMAND ----------

# MAGIC %md ## Step 1: Calculate Current Inventory (Batch)
# MAGIC 
# MAGIC To get started with our current inventory calculations, we might first approach the problem using a traditional, run-once SQL statement.  Given a table *pos.latest_inventory_snapshot* containing the most recent physical count of a product in a given store and a set of inventory-relevant change records in a table named *pos.inventory_change*, we might express our logic as follows:

# COMMAND ----------

# DBTITLE 1,SQL-Logic for Current Inventory
# MAGIC %sql
# MAGIC 
# MAGIC SELECT  -- calculate current inventory
# MAGIC   a.store_id,
# MAGIC   a.item_id,
# MAGIC   FIRST(a.quantity) as snapshot_quantity,  
# MAGIC   COALESCE(SUM(b.quantity),0) as change_quantity,
# MAGIC   FIRST(a.quantity) + COALESCE(SUM(b.quantity),0) as current_inventory,
# MAGIC   GREATEST(FIRST(a.date_time), MAX(b.date_time)) as date_time
# MAGIC FROM pos.latest_inventory_snapshot a  -- access latest snapshot
# MAGIC LEFT OUTER JOIN ( -- calculate inventory change with bopis corrections
# MAGIC   SELECT
# MAGIC     x.store_id,
# MAGIC     x.item_id,
# MAGIC     x.date_time,
# MAGIC     x.quantity
# MAGIC   FROM pos.inventory_change x
# MAGIC   INNER JOIN pos.store y
# MAGIC     ON x.store_id=y.store_id
# MAGIC   INNER JOIN pos.inventory_change_type z
# MAGIC     ON x.change_type_id=z.change_type_id
# MAGIC   WHERE NOT(y.name='online' AND z.change_type='bopis') -- exclude bopis records from online store
# MAGIC   ) b
# MAGIC   ON 
# MAGIC     a.store_id=b.store_id AND 
# MAGIC     a.item_id=b.item_id AND 
# MAGIC     a.date_time<=b.date_time
# MAGIC GROUP BY
# MAGIC   a.store_id,
# MAGIC   a.item_id
# MAGIC ORDER BY 
# MAGIC   date_time DESC

# COMMAND ----------

# MAGIC %md A few things about this query:
# MAGIC 
# MAGIC 1. *BOPIS* events are recorded in both the online and the physical store within which the transaction is fulfilled.  For inventory purposes, the online version of these records are ignored.  We realize this is a very simplistic approach to the handling of these types of transactions. The purpose here is to illustrate how data changing with different cadences can be combined to produce a query result.
# MAGIC 
# MAGIC 2. The join logic between the latest inventory snapshot and BOPIS-corrected inventory change data aligns snapshot counts with event change records occurring at or after the time of the snapshot.  With each new snapshot, the change event records required to calculate the current state changes.

# COMMAND ----------

# MAGIC %md While we can express both streaming and batch logic using SQL, many prefer to implement such logic in Python.  Here is the same logic as in the SQL statement expressed using Python.  Please note that the aliasing used in the Python code is aligned with that of the SQL statement for easier comparisons:

# COMMAND ----------

# DBTITLE 1,Python-Logic for Current Inventory Calculation
# calculate inventory change with bopis corrections
inventory_change = (
  spark.table('pos.inventory_change').alias('x')
    .join(spark.table('pos.store').alias('y'), on='store_id')
    .join(spark.table('pos.inventory_change_type').alias('z'), on='change_type_id')
    .filter(f.expr("NOT(y.name='online' AND z.change_type='bopis')"))
    .select('store_id','item_id','date_time','quantity')
  )

# access latest snapshot
inventory_snapshot = spark.table('pos.latest_inventory_snapshot')

# calculate current inventory
inventory_current = (
  inventory_snapshot.alias('a')
    .join(
      inventory_change.alias('b'), 
      on=f.expr('''
        a.store_id=b.store_id AND 
        a.item_id=b.item_id AND 
        a.date_time<=b.date_time
        '''), 
      how='leftouter'
      )
    .groupBy('a.store_id','a.item_id')
      .agg(
        f.first('a.quantity').alias('snapshot_quantity'),
        f.sum('b.quantity').alias('change_quantity'),
        f.first('a.date_time').alias('snapshot_datetime'),
        f.max('b.date_time').alias('change_datetime')
        )
    .withColumn('change_quantity', f.coalesce('change_quantity', f.lit(0)))
    .withColumn('current_quantity', f.expr('snapshot_quantity + change_quantity'))
    .withColumn('date_time', f.expr('GREATEST(snapshot_datetime, change_datetime)'))
    .drop('snapshot_datetime','change_datetime')
    .orderBy('current_quantity')
  )

display(inventory_current)

# COMMAND ----------

# MAGIC %md ## Step 2: Calculate Current Inventory (Structured Streaming)
# MAGIC 
# MAGIC Having worked out the logic for a batch calculation, let's take a look at how we might tackle this as a streaming calculation.  Starting with the inventory change data, we will read this as a stream using logic very similar to the static logic defined above:

# COMMAND ----------

# DBTITLE 1,Access Inventory Change Stream
inventory_change = (
  (
    spark  # read inventory change data as a stream source
      .readStream
      .format('delta')
      .table('pos.inventory_change')
      .alias('x')
    )
    .join(spark.table('pos.store').alias('y'), on='store_id')
    .join(spark.table('pos.inventory_change_type').alias('z'), on='change_type_id')
    .filter(f.expr("NOT(y.name='online' AND z.change_type='bopis')"))
    .select('store_id','item_id','date_time','quantity')
  )

display(inventory_change)

# COMMAND ----------

# MAGIC %md And now we access the latest snapshot data.  Notice that we are reading this as a traditional static DataFrame, identical to how we did before: 

# COMMAND ----------

# DBTITLE 1,Access Latest Inventory Snapshots
inventory_snapshot = spark.table('pos.latest_inventory_snapshot')

display(inventory_snapshot)

# COMMAND ----------

# MAGIC %md Now we can bring these datasets together to define our current inventory standing query for stream processing.  This query looks identical to the code in the last section of this notebook with **one big exception**: we are using an **INNER JOIN**.  
# MAGIC 
# MAGIC One of the limitations of structured streaming as it is implemented today is that while you can join streaming and static datasets, you cannot do so using an outer join if the streaming dataset is on the optional side of that join.
# MAGIC 
# MAGIC To overcome this problem, you may remember in the last notebook that we provided logic with the custom *upsertToDelta* function to insert *dummy records* into the *pos.inventory_change* table as new snapshot data was processed.  Those *dummy records* ensure that a change record exists with each item that comes through a snapshot.  The use of a 0-valued quantity for those records ensures the result of the summation is unaffected.  It's a little hack but it solves our problem while also returning us the correct result:

# COMMAND ----------

# DBTITLE 1,Logic for Streaming Current Inventory Calculation
inventory_current = (  
  inventory_snapshot.alias('a')
    .join(
      inventory_change.alias('b'), 
      on=f.expr('''
        a.store_id = b.store_id AND 
        a.item_id = b.item_id AND 
        a.date_time <= b.date_time
        '''), 
      how='inner'  # use of inner join requires generation of 0 quantity records from pos (see ETL notebook)
      )
    .groupBy('a.store_id','a.item_id')
      .agg(
        f.first('a.quantity').alias('snapshot_quantity'),
        f.sum('b.quantity').alias('change_quantity'),
        f.first('a.date_time').alias('snapshot_datetime'),
        f.max('b.date_time').alias('change_datetime')
        )
    .withColumn('current_quantity', f.expr('snapshot_quantity + change_quantity'))
    .withColumn('date_time', f.expr('GREATEST(snapshot_datetime, change_datetime)'))
    .drop('snapshot_datetime','change_datetime')
  )

display(inventory_current.orderBy('date_time', ascending=False))

# COMMAND ----------

# MAGIC %md Its important to note that as new change records move through the standing query, the DataFrame maintains awareness of any updates to the snapshot table, even though that table was defined using a static DataFrame.  This is one of the benefits of using Delta as its underlying format.
# MAGIC 
# MAGIC With our logic defined, we will flow our data into a Delta table named *pos.inventory_current*.  This table can be used as the source for various reports and dashboards.  It will be refreshed completely every 15-minutes based on a trigger defined with the table write:

# COMMAND ----------

# DBTITLE 1,Write Current Inventory
# create destination table if not exists
_ = spark.sql('''
  CREATE TABLE IF NOT EXISTS pos.inventory_current (
    store_id int,
    item_id int,
    snapshot_quantity int,
    change_quantity bigint,
    current_quantity bigint,
    date_time timestamp
    )
    USING DELTA
  '''
  )

# stream data into destination table on 15-minute interval
_ = (
  inventory_current
    .writeStream
    .format('delta')
    .outputMode('complete')
    .option('checkpointLocation',config['inventory_current_checkpoint_path'])
    .trigger(processingTime='15 minutes')
    .table('pos.inventory_current')
  )

# COMMAND ----------

# MAGIC %md ## Step 3: Calculate Current Inventory (Delta Live Tables)
# MAGIC 
# MAGIC At the time of notebook development, Databricks announced a new featured called [**Delta Live Tables**](https://databricks.com/product/delta-live-tables) which looks to greatly simplify the development and execution of streaming queries like the one used to define current inventory. As the feature is (as of the time of development) in private preview, it is too soon for us to provide prescriptive guidance on how this feature could be employed to tackle this scenario.  However, this is a topic we intend to revisit as it moves closer to general availability.

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
