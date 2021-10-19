# Databricks notebook source
# MAGIC %run ./feature_dict

# COMMAND ----------

# MAGIC %run ./factory

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

# MAGIC %md Prepare src tables from TPC-DS
# MAGIC 
# MAGIC Before running this demo notebook, source delta tables need to be created from TPC-DS needs using ../tpcds_datagen

# COMMAND ----------

#The path where the delta data is stored
store_sales_delta_path = "delta table/location generated using the tpcds_datagen notebook"
src_df = spark.table(f"delta.`{store_sales_delta_path}`")

# COMMAND ----------

features = StoreSales()
fv_months = features.total_sales.multiply("i_category", ["Music", "Home", "Shoes"]).multiply("month_id", [200012, 200011, 200010])
df = append_features(src_df, [features.collector], fv_months)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md one-hot encoding

# COMMAND ----------

src_df = spark.createDataFrame([(1, "iphone"), (2, "samsung"), (3, "htc"), (4, "vivo")], ["user_id", "device_type"])
encode = Feature(_name="device_type_encode", _base_col=f.lit(1), _negative_value=0)
onehot_encoder = encode.multiply("device_type", ["iphone", "samsung", "htc", "vivo"])
df = append_features(src_df, ["device_type"], onehot_encoder)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md minhash
# MAGIC 
# MAGIC Minhash implementation needs to be installed from https://pypi.org/project/datasketch/
# MAGIC 
# MAGIC While Minhash is useful for estimating large distinct count, the small source dataset is only used here to demo how MinHash can be applied.

# COMMAND ----------

from hashlib import sha1
from datasketch.minhash import MinHash
from datasketch.lean_minhash import LeanMinHash
import numpy as np

@pandas_udf("binary")
def create_minhash(v: pd.Series) -> bytearray:
  mh = MinHash(num_perm=64)
  for val in v:
    if val: mh.update(val.encode('utf8'))
  lean_minhash = LeanMinHash(mh)
  buf = bytearray(lean_minhash.bytesize())
  lean_minhash.serialize(buf)
  return buf

# COMMAND ----------

@pandas_udf("integer")
def minhash_cardinality(mh: pd.Series) -> pd.Series:
  result = []
  for buf in mh:
    lmh = LeanMinHash.deserialize(buf)
    result.append(round(lmh.count()))
  return pd.Series(result)

# COMMAND ----------

@pandas_udf("integer")
def minhash_intersect(mh1: pd.Series, mh2: pd.Series) -> pd.Series:
  result = []
  N = len(mh1)
  for i in range(N):
    buf1, buf2 = mh1[i], mh2[i]
    lmh1 = LeanMinHash.deserialize(buf1)
    lmh2 = LeanMinHash.deserialize(buf2)
    jac = lmh1.jaccard(lmh2)
    lmh1.merge(lmh2)
    intersect_cnt = jac * lmh1.count()
    result.append(round(intersect_cnt))
  return pd.Series(result)

# COMMAND ----------

src_df = spark.table(f"delta.`{store_sales_delta_path}`")

# COMMAND ----------

src_df.printSchema()

# COMMAND ----------

# MAGIC %md As an exercise, you can figure out how `create_minhash` can be defined as _agg_func method of a BaseFeature

# COMMAND ----------

import pyspark.sql.functions as f
grocery_1m_trans_df = src_df.groupby("ss_customer_sk").agg(
  create_minhash(f.when(f.col("i_category")=="Music", f.concat("ss_ticket_number", "d_date")).otherwise(None)).alias("music_trans_minhash"),
  create_minhash(f.when(f.col("month_id")==200012, f.concat("ss_ticket_number", "d_date")).otherwise(None)).alias("trans_1m_minhash")
)

# COMMAND ----------

result_df = grocery_1m_trans_df.select("ss_customer_sk", minhash_intersect("music_trans_minhash", "trans_1m_minhash").alias("total_trans_music_1m"))

# COMMAND ----------

display(result_df)
