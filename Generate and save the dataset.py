# Databricks notebook source
# MAGIC %md
# MAGIC # Generate a dataset and save to Delta tables
# MAGIC This notebook generates five Delta tables:
# MAGIC * `user_profile`: user_id and their static profiles
# MAGIC * `item_profile`: item_id and their static profiles
# MAGIC * `user_item_interaction`: events when a user interacts with an item
# MAGIC   * this table is randomly split into three tables for model training and evaluation: `train`, `val`, and `test`
# MAGIC 
# MAGIC For simplicity, the user and item profiles contain only two attributes: age and topic. The `user_age` column is the user's age, and the `item_age` column is the average age of users who interact with the item. The `user_topic` column is the user's favorite topic, and the `item_topic` column is the most relevant topic of the item. Also for simplicity, the `user_item_interaction` table ignores event timestamps and includes only `user_id`, `item_id`, and a binary label column repreesenting whether the user interacts with the item. 
# MAGIC 
# MAGIC ## How the label is calculated               
# MAGIC This notebook randomly assigns a label representing whether the user interacts with the item. The label is based on the similarity of the user and item, which is determined by their age and topic attributes.
# MAGIC 
# MAGIC The calculation divides users into three age ranges: under 18, 18-34, 35-60. If `user_age` and `item_age` are in the same range, the probability of iteraction is higher.
# MAGIC 
# MAGIC * same age range: P(interact_age) = 0.3
# MAGIC * different age range: P(interact_age) = 0.1
# MAGIC 
# MAGIC The topic has 10 categories. This calculation assumes that topics (1,2,4) are related, (3,6,9) are related, and (0,5,7,8) are related.
# MAGIC 
# MAGIC * related topic: P(interact_topic) = 0.3
# MAGIC * different topic: P(interact_topic) = 0.05
# MAGIC 
# MAGIC The overall probability that a user interacts with an item is P(interact) = P(interact_age OR interact_topic). This notebook randomly generates a label based on that probability.

# COMMAND ----------

import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split   

from pyspark.sql.functions import *

# COMMAND ----------

NUM_USERS = 400
NUM_ITEMS = 2000
NUM_INTERACTIONS = 4000
NUM_TOPICS = 10
MAX_AGE = 60

DATA_DBFS_ROOT_DIR = '/tmp/recommender/data'

def export_pd_in_delta(pdf, name):
  spark.createDataFrame(pdf).write.format("delta").mode("overwrite").save(f"{DATA_DBFS_ROOT_DIR}/{name}")

# COMMAND ----------

# MAGIC %md # Generate features

# COMMAND ----------

user_pdf = pd.DataFrame({
  "user_id": [i for i in range(NUM_USERS)],
  "user_age": [np.random.randint(MAX_AGE) for _ in range(NUM_USERS)],
  "user_topic": [np.random.randint(NUM_TOPICS) for _ in range(NUM_USERS)],
})

# COMMAND ----------

item_pdf = pd.DataFrame({
  "item_id": [i for i in range(NUM_ITEMS)],
  "item_age": [np.random.randint(MAX_AGE) for _ in range(NUM_ITEMS)],
  "item_topic": [np.random.randint(NUM_TOPICS) for _ in range(NUM_ITEMS)],
})

# COMMAND ----------

export_pd_in_delta(item_pdf, "item_profile")
export_pd_in_delta(user_pdf, "user_profile")

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate labels

# COMMAND ----------

user_id = [np.random.randint(NUM_USERS) for _ in range(NUM_INTERACTIONS)]
item_id = [np.random.randint(NUM_ITEMS) for _ in range(NUM_INTERACTIONS)]
pdf = pd.DataFrame({"user_id": user_id, "item_id": item_id})

# COMMAND ----------

all_pdf = pdf \
    .set_index('item_id') \
    .join(item_pdf.set_index('item_id'), rsuffix='_it').reset_index() \
    .set_index('user_id') \
    .join(user_pdf.set_index('user_id'), rsuffix='_us').reset_index()

# COMMAND ----------

display(all_pdf)

# COMMAND ----------

def get_range(age):
  # <18, 18-34, 35-60
  if age < 18:
    return 0
  if age < 35:
    return 1
  return 2

#  (1,2,4) are related, (3,6,9) are related, (0,5,7,8) are related.
d = {1:0, 2:0, 4:0, 3:1, 6:1, 9:1, 0:2, 5:2, 7:2, 8:2}
  
def calc_clicked(ad_age, ad_topic, disp_age, disp_topic):
  if get_range(ad_age) == get_range(disp_age):
    age_not_click = 0.7
  else:
    age_not_click = 0.9
  if d[ad_topic] == d[disp_topic]:
    disp_not_click = 0.7
  else:
    disp_not_click = 0.95
  overall_click = 1 - age_not_click * disp_not_click
  return 1 if np.random.rand() < overall_click else 0

# COMMAND ----------

all_pdf['label'] = all_pdf.apply(lambda row: calc_clicked(
  row['item_age'], row['item_topic'], row['user_age'], row['user_topic']
), axis=1)

# COMMAND ----------

export_pdf = all_pdf[['user_id', 'item_id', 'label']]
display(export_pdf)

# COMMAND ----------

export_pdf.groupby(['user_id']).sum().describe()[['label']]

# COMMAND ----------

export_pdf.groupby(['user_id', 'item_id']).sum().describe()

# COMMAND ----------

train, test = train_test_split(export_pdf, test_size=0.2)
train, val = train_test_split(train, test_size=0.2)
print(len(train), 'train examples')
print(len(val), 'validation examples')
print(len(test), 'test examples')
export_pd_in_delta(train, "user_item_interaction_train")
export_pd_in_delta(val, "user_item_interaction_val")
export_pd_in_delta(test, "user_item_interaction_test")
