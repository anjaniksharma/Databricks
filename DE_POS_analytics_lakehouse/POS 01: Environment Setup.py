# Databricks notebook source
# MAGIC %md This notebook was developed for use with a **Databricks 8.3 cluster**. It provides instructions on the setup of the environment required for the point-of-sale streaming demonstration delivered through the three remaining *POS* notebooks.  You do not need to run this notebook separately as each notebook in the set calls this notebook to retrieve configuration data. 
# MAGIC 
# MAGIC However, you should read the environment setup instructions in this notebook carefully and update all configuration values as appropriate.
# MAGIC 
# MAGIC **NOTE** For ease of setup, we are recording various connection strings and keys in plain text.  For environments with sensitive data, it is advised that you take advantage of the Databricks [secret management](https://docs.databricks.com/security/secrets/index.html) capability to secure these values.

# COMMAND ----------

# DBTITLE 1,Initialize Config Settings
if 'config' not in locals():
  config = {}

# COMMAND ----------

# MAGIC %md ## Step 1: Setup the Azure Environment
# MAGIC 
# MAGIC While Databricks is a cloud-agnostic platform, this demonstration will leverage several external technologies made available by the cloud-provider. This will require us to provide cloud-specific environment setup guidance. For this demonstration, we've elected to make use of technologies provided by the Microsoft Azure cloud though this scenario is supportable using similar technologies made available by AWS and GCP.
# MAGIC 
# MAGIC The Azure-specific technologies we will use are:</p>
# MAGIC 
# MAGIC * [Azure IOT Hub](https://docs.microsoft.com/en-us/azure/iot-hub/about-iot-hub)
# MAGIC * [Azure Storage](https://docs.microsoft.com/en-us/azure/storage/common/storage-introduction)
# MAGIC 
# MAGIC To set these up, you will need to have access to an [Azure subscription](https://azure.microsoft.com/en-us/account/).  

# COMMAND ----------

# MAGIC %md #### Step 1a: Setup the Azure IOT Hub
# MAGIC 
# MAGIC To setup and configure the Azure IOT Hub, you will need to:</p>
# MAGIC 
# MAGIC 1. [Create an Azure IOT Hub](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-create-through-portal) (We used an S1-sized IOT Hub for a 10x playback of event data as described in the next notebook.)
# MAGIC 2. [Add an Edge Device](https://docs.microsoft.com/en-us/azure/iot-edge/how-to-register-device?view=iotedge-2020-11&tabs=azure-portal)  (We used a device with Symmetric key authentication and auto-generated keys enabled to connect to the IOT Hub.)
# MAGIC 3. [Retrieve the Edge Device connection string](https://docs.microsoft.com/en-us/azure/iot-edge/how-to-register-device?view=iotedge-2020-11&tabs=azure-portal#view-registered-devices-and-retrieve-connection-strings)
# MAGIC 4. [Retrieve the Azure IOT Hub's Event Hub Endpoint connection string](https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-messages-read-builtin#read-from-the-built-in-endpoint)
# MAGIC 5. Record Azure IOT Hub relevant configuration values in the cell below:

# COMMAND ----------

# DBTITLE 1,Config Settings for Azure IOT Hub 
config['iot_device_connection_string'] = 'HostName=posiot.azure-devices.net;DeviceId=posiotdevice;SharedAccessKey=Ye/vmbsCOGZHs9bukQ7rLEV5fU+851H8cimKO4wHVTc='

config['event_hub_connection_string'] = 'HostName=posiot.azure-devices.net;SharedAccessKeyName=service;SharedAccessKey=toEpyC06aahLZYXAjgRgy+Ih+qf0uRNS6YKYNCwZSCo='

# COMMAND ----------

# MAGIC %md ####Step 1b: Setup the Azure Storage Account
# MAGIC 
# MAGIC To setup and configure the Azure Storage account, you will need to:</p>
# MAGIC 
# MAGIC 1. [Create an Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal)
# MAGIC 2. [Create a Blob Storage container](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal)
# MAGIC 3. [Retrieve an Account Access Key & Connection String](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys)
# MAGIC 4. Record Azure Storage Account relevant configuration values in the cell below:

# COMMAND ----------

# DBTITLE 1,Config Settings for Azure Storage Account
config['storage_account_name'] = 'posanalysis'
config['storage_container_name'] = 'posfiles'
config['storage_account_access_key'] = '1KoGkMGScnFqeqoJJDQ9HgUBcLHel798LtR6e3yy/xDugn8Jdkme0WCGiI5n6+vqfYsDE7Wea5qqVOInlkVInw=='
config['storage_connection_string'] = 'DefaultEndpointsProtocol=https;AccountName=posanalysis;AccountKey=1KoGkMGScnFqeqoJJDQ9HgUBcLHel798LtR6e3yy/xDugn8Jdkme0WCGiI5n6+vqfYsDE7Wea5qqVOInlkVInw==;EndpointSuffix=core.windows.net'

# COMMAND ----------

# MAGIC %md ## Step 2: Mount the Azure Storage to Databricks
# MAGIC 
# MAGIC The Azure Storage account created in the previous step will now be mounted to the Databricks environment.  To do this, you will need to:</p>
# MAGIC 
# MAGIC 1. Access the Azure Storage Account through the Azure Portal
# MAGIC 2. [Copy the Account's primary or secondary key](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys)
# MAGIC 3. [Mount the Account to the Databricks File System (DBFS)](https://docs.databricks.com/data/data-sources/azure/azure-storage.html#mount-azure-blob-storage-containers-to-dbfs)

# COMMAND ----------

# DBTITLE 1,Config Settings for DBFS Mount Point
config['dbfs_mount_name'] = '/mnt/pos'

# COMMAND ----------

# DBTITLE 1,Create DBFS Mount Point
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

dbutils.fs.mounts()

# COMMAND ----------

! ls -lrt /dbfs/mnt/pos

# COMMAND ----------

# MAGIC %md ## Step 3: Upload Data Files to Azure Storage
# MAGIC 
# MAGIC The demonstration makes use of simulated data that can be downloaded to your desktop system from [here](https://github.com/databricks/tech-talks/blob/master/datasets/point_of_sale_simulated.zip). To make this data available to the notebooks, the various files must be uploaded to the DBFS mount point created in the last step as follows:
# MAGIC 
# MAGIC **NOTE** In the table below, it is assumed the mount point is defined as */mnt/pos*. If you used an alternative mount point name above, be sure you updated the *dbfs_mount_name* configuration parameter and deposit the files into the appropriate location.
# MAGIC 
# MAGIC | File Type | File | Path |
# MAGIC | -----|------|
# MAGIC | Change Event| inventory_change_store001.txt |  /mnt/pos/generator/inventory_change_store001.txt |
# MAGIC |Change Event| inventory_change_online.txt | /mnt/pos/generator/inventory_change_online.txt  |
# MAGIC |Snapshot| inventory_snapshot_store001.txt | /mnt/pos/generator/inventory_snapshot_store001.txt  |
# MAGIC |Snapshot| inventory_snapshot_online.txt | /mnt/pos/generator/inventory_snapshot_online.txt  |
# MAGIC |Static |      stores.txt                         |         /mnt/pos/static_data/stores.txt                                         |
# MAGIC |Static|      items.txt                 |           /mnt/pos/static_data/items.txt                                       |
# MAGIC |Static|      inventory_change_type.txt                      |           /mnt/pos/static_data/inventory_change_type.txt                                       |
# MAGIC 
# MAGIC To upload these files from your desktop system, please feel free to use either of the following techniques:</p>
# MAGIC 
# MAGIC 1. [Upload files via the Azure Portal](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal)
# MAGIC 2. [Upload files via Azure Storage Explorer](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-storage-explorer)

# COMMAND ----------

# DBTITLE 1,Config Settings for Data Files
# change event data files
config['inventory_change_store001_filename'] = config['dbfs_mount_name'] + '/generator/inventory_change_store001.txt'
config['inventory_change_online_filename'] = config['dbfs_mount_name'] + '/generator/inventory_change_online.txt'

# snapshot data files
config['inventory_snapshot_store001_filename'] = config['dbfs_mount_name'] + '/generator/inventory_snapshot_store001.txt'
config['inventory_snapshot_online_filename'] = config['dbfs_mount_name'] + '/generator/inventory_snapshot_online.txt'

# static data files
config['stores_filename'] = config['dbfs_mount_name'] + '/static_data/store.txt'
config['items_filename'] = config['dbfs_mount_name'] + '/static_data/item.txt'
config['change_types_filename'] = config['dbfs_mount_name'] + '/static_data/inventory_change_type.txt'

# COMMAND ----------

# MAGIC %md ## Step 4: Configure Misc. Items
# MAGIC 
# MAGIC Last, we will provide the paths to a few items our demonstration will need to access.  First amongst these is the location of the inventory snapshot files our simulated stores will deposit into our streaming infrastructure.  This path should be dedicated to this one purpose and not shared with other file types:

# COMMAND ----------

# DBTITLE 1,Config Settings for Checkpoint Files
config['inventory_snapshot_path'] = config['dbfs_mount_name'] + '/inventory_snapshots/'

# COMMAND ----------

# MAGIC %md Next, we will configure the locations for the various checkpoint files our streaming jobs will employ.  Again, these paths should be dedicated to this function and not be used for other files or processes:

# COMMAND ----------

# DBTITLE 1,Config Settings for Checkpoint Files
config['inventory_change_checkpoint_path'] = config['dbfs_mount_name'] + '/checkpoints/inventory_change'
config['inventory_snapshot_checkpoint_path'] = config['dbfs_mount_name'] + '/checkpoints/inventory_snapshot'
config['inventory_current_checkpoint_path'] = config['dbfs_mount_name'] + '/checkpoints/inventory_current'

# COMMAND ----------

config

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
