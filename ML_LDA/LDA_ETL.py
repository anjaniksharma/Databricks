# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ### Topic modeling using PySpark and the Pyspark.ml library. 
# MAGIC 
# MAGIC We also use Feature store to save the extracted features to perform the ETL so that we can save and reuse the cleaned features for experimentation. This makes it easier to experiment using various topic modeling algorithms and perform hyperparameter optimization on them.
# MAGIC 
# MAGIC 1. Read the JSON data
# MAGIC 2. Clean and transform the data to generate the text features
# MAGIC 3. Create the feature store database
# MAGIC 4. Write the generated features to the feature store
# MAGIC 5. Load the features from the feature store and perform topic modeling

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Perform the ETL on the data to clean and extract text features

# COMMAND ----------

from databricks import feature_store
fs = feature_store.FeatureStoreClient()

df = spark.read.format("json").load("/FileStore/*.txt")
pub_extracted = df.rdd.map(lambda x: ( x['user']['screen_name'], x['id'], x['full_text']) ).toDF(['name','tweet_id','text'])
pub_sentences_unique = pub_extracted.dropDuplicates(['tweet_id'])
pub_sentences_unique.show(truncate=True)
pub_sentences_unique.count()

# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer, StopWordsRemover
from pyspark.sql.functions import *

tokenizer = Tokenizer(inputCol="text", outputCol="words")
wordsData = tokenizer.transform(pub_sentences_unique)

remover = StopWordsRemover(inputCol="words", outputCol="filtered")
filtered = remover.transform(wordsData)

cv = CountVectorizer(inputCol="filtered", outputCol="rawFeatures", vocabSize=5000, minDF=10.0)
cvmodel = cv.fit(filtered)
vocab = cvmodel.vocabulary
featurizedData = cvmodel.transform(filtered)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
rescaledData = rescaledData.withColumn('stringFeatures', rescaledData.rawFeatures.cast(StringType()))
rescaledData = rescaledData.withColumn('coltext', concat_ws(',', 'filtered' ))
rescaledData.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create the feature store database and write the features from the transformed dataframe

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS lda_example2;

# COMMAND ----------

fs.create_feature_table(name = "lda_example2.rescaled_features", keys = ['tweet_id', 'text', 'coltext', 'stringFeatures'], features_df = rescaledData.select('tweet_id', 'text', 'coltext', 'stringFeatures'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Perform Topic modeling using LDA by reading the features from the saved features

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.clustering import LDA
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
import datetime
from databricks import feature_store

fs = feature_store.FeatureStoreClient()

yesterday = datetime.date.today() + datetime.timedelta(seconds=36000)

# Read feature values 
lda_features_df = fs.read_table(
  name='lda_example2.rescaled_features',
  #as_of_delta_timestamp=str(yesterday)
)

lda_features_df.show()
df_new = lda_features_df.withColumn("s", expr("split(substr(stringFeatures,2,length(stringFeatures)-2), ',\\\\s*(?=\\\\[)')")) \
  .selectExpr("""
      concat(
        /* type = 0 for SparseVector and type = 1 for DenseVector */
        '[{"type":0,"size":',
        s[0],
        ',"indices":',
        s[1],
        ',"values":',
        s[2],
        '}]'
      ) as vec_json
   """) \
  .withColumn('features', from_json('vec_json', ArrayType(VectorUDT()))[0])

# ------------------ Perform LDA and compute Likelihood and Perplexity ------------------ #

lda_model = LDA(k=10, maxIter=20)
model = lda_model.fit(df_new)
lda_data = model.transform(df_new)

ll = model.logLikelihood(lda_data)
lp = model.logPerplexity(lda_data)
print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
print("The upper bound on perplexity: " + str(lp))

# -------------- Read vocab and map words to topics ---------------- #
vocab_read = spark.read.format("delta").load("/tmp/cvvocab")
vocab_read_list = vocab_read.toPandas()['vocab'].values
vocab_broadcast = sc.broadcast(vocab_read_list)
topics = model.describeTopics()

def map_termID_to_Word(termIndices):
    words = []
    for termID in termIndices:
        words.append(vocab_broadcast.value[termID])

    return words

udf_map_termID_to_Word = udf(map_termID_to_Word , ArrayType(StringType()))

ldatopics_mapped = topics.withColumn("topic_desc", udf_map_termID_to_Word(topics.termIndices))

topics_df = ldatopics_mapped.select(col("termweights"), col("topic_desc")).toPandas()
display(topics_df)


# COMMAND ----------

import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots

fig = make_subplots(rows=10, cols=1, start_cell="bottom-left")

for i in range(10):
  fig.add_trace(px.bar(x=topics_df['topic_desc'].iloc[i], y=topics_df['termweights'].iloc[i])['data'][0], row=i+1, col=1)
  
fig.update_layout(height=1400, width=1050)
fig.show()



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Rewrite feature store if you need to update the schema

# COMMAND ----------

fs.write_table(name = "lda_example.rescaled_features", mode= 'overwrite', df = rescaledData.select('name','tweet_id', 'text', 'coltext','stringFeatures'))

# COMMAND ----------

reread_features = fs.read_table(
  name='lda_example.rescaled_features',
  #as_of_delta_timestamp=str(yesterday)
)

# COMMAND ----------

reread_features.show()
