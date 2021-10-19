# Databricks notebook source
# MAGIC %run ./core_feature_factory/feature_dict

# COMMAND ----------

# MAGIC %run ./core_feature_factory/factory

# COMMAND ----------

# MAGIC %run ./core_feature_factory/functions

# COMMAND ----------

# MAGIC %md Prepare src tables from TPC-DS
# MAGIC 
# MAGIC Before running this demo notebook, source delta tables need to be created from TPC-DS needs using the tpcds_datagen notebook.

# COMMAND ----------

store_sales_delta_path = "delta table/location generated using the tpcds_datagen notebook"

# COMMAND ----------

src_df = spark.table(f"delta.`{store_sales_delta_path}`")

# COMMAND ----------

features = StoreSales()
fv_months = features.total_sales.multiply("i_category", ["Music", "Home", "Shoes"]).multiply("month_id", [200012, 200011, 200010])
df = append_features(src_df, [features.collector], fv_months)

# COMMAND ----------

display(df)

# COMMAND ----------


from databricks.feature_store import feature_table

@feature_table
def compute_customer_features(data):
  features = StoreSales()
  fv_months = features.total_sales.multiply("i_category", ["Music", "Home", "Shoes"]).multiply("month_id", [200012, 200011, 200010])
  df = append_features(src_df, [features.collector], fv_months)
  return df



# COMMAND ----------

# %sql CREATE DATABASE IF NOT EXISTS trust_and_safety;

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()
customer_features_df = compute_customer_features(src_df)


# COMMAND ----------

display(customer_features_df)

# COMMAND ----------

from databricks import feature_store

fs = feature_store.FeatureStoreClient()
fs.create_feature_table(
    name="jumpstart_db.customer_features",
    keys=["customer_id"],
    features_df=customer_features_df,
    description="customer feature table",
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jumpstart_db.customer_features
