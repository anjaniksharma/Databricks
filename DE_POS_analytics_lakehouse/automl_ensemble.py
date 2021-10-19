# Databricks notebook source
# MAGIC %md
# MAGIC ## Ensemble AutoML 
# MAGIC ### Pre-requisites
# MAGIC * Databricks Runtime 8.3 ML or above
# MAGIC ##### Hypothesis
# MAGIC * Traditionally ensembles are more robust and have better performance but are more difficult o build and maintain, but autoML takes on that burden from the user
# MAGIC * AutoML no only ndicates the best model but also provides a summary of all models trained 
# MAGIC * This offers a low hanging opportunity for the user to check if an ensemble model performs better than a single model
# MAGIC * It is especially easy to test that option since all the model runs are available.
# MAGIC ##### Dataset
# MAGIC * In this example dataset, we are predicting potential customer churn in a [telco dataset](https://www.kaggle.com/blastchar/telco-customer-churn) from Kaggle

# COMMAND ----------

# DBTITLE 1,Load Raw Data, divide into train and test, select the target column to use for prediction
input_df = spark.table("ibm_telco_churn.bronze_customers")
train_df, test_df = input_df.randomSplit([0.90, 0.1], seed=42)
#use later for additional inference testing
new_df = test_df 
display(train_df)

# COMMAND ----------

# DBTITLE 1,Encoding (churnString to churn numeric)
from pyspark.sql.functions import when, col
test_df = test_df.withColumn("churn", when(test_df.churnString == 'Yes' ,1).otherwise(0))
train_df = train_df.withColumn("churn", when(test_df.churnString == 'Yes' ,1).otherwise(0))

# COMMAND ----------

# DBTITLE 1,Drop the predicted value for testing later in the notebook
import sklearn.metrics
import numpy as np
test_pdf = test_df.toPandas()
y_test = test_pdf["churn"]
X_test = test_pdf.drop("churn", axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Train

# COMMAND ----------

# DBTITLE 1,Use AutoML to train models by specifying the target column and expectations around training time
import databricks.automl
data_dir = "dbfs:/tmp/ensemble_automl/"
dbutils.fs.rm(data_dir, True)
automl_models = databricks.automl.classify(train_df, 
                                   target_col = "churn",
                                   data_dir= data_dir,
                                   timeout_minutes=60, 
                                   max_trials=1000) 

# COMMAND ----------

automl_models

# COMMAND ----------

# DBTITLE 1,Get the Experiment Id
import mlflow
from mlflow.tracking.client import MlflowClient

client = MlflowClient()
experiment_id = automl_models.experiment.experiment_id
experiment_id

# COMMAND ----------

# DBTITLE 1,Determine the best model 
print(automl_models.best_trial.model_description)
best_model_uri = automl_models.best_trial.model_path
metrics = automl_models.best_trial.metrics
print('accuracy=', metrics['val_accuracy_score'], ' f1 score=', metrics['val_f1_score'], ' precision=', metrics['val_precision_score'],  \
                ' recall=',metrics['val_recall_score'],  ' roc_auc_score=',metrics['val_roc_auc_score'])
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=best_model_uri, result_type="integer")
test_df = test_df.withColumn("bestModel", predict_udf())
display(test_df)

# COMMAND ----------

# DBTITLE 1,Fetch Confusion Matrix for Best Model
model = mlflow.sklearn.load_model(best_model_uri)
sklearn.metrics.plot_confusion_matrix(model, X_test, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Determine the top models from all the different architectures in the experimental runs

# COMMAND ----------

# DBTITLE 1,Generate Confusion Matrix for Best Models in the Different Architectures by F1 Score
model_types = ['DecisionTree', 'LogisticRegression', 'RandomForest', 'LGBM', 'XGB']
for model_type in model_types:
  filter_str = "params.classifier LIKE '" + model_type + "%'"
  print(filter_str)
  
  models = (client.search_runs(experiment_ids=experiment_id, filter_string=filter_str, order_by=["metrics.val_f1_score DESC"]))
  
  
  if models:
    model = models[0]
    metrics = model.data.metrics
    print('accuracy=', metrics['val_accuracy_score'], ' f1 score=', metrics['val_f1_score'], ' precision=', metrics['val_precision_score'],  \
                ' recall=',metrics['val_recall_score'],  ' roc_auc_score=',metrics['val_roc_auc_score'])
    best_runId = model.info.run_uuid
    model_uri = f"runs:/{best_runId}/model"

    predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="integer")
    test_df = test_df.withColumn(model_type, predict_udf())
    
    model = mlflow.sklearn.load_model(model_uri)  
    disp = sklearn.metrics.plot_confusion_matrix(model, X_test, y_test)
    disp.ax_.set_title(model_type)

display(test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensemble Options

# COMMAND ----------

# MAGIC %md 
# MAGIC #### How many models?
# MAGIC 
# MAGIC ##### Voting Strategy
# MAGIC 
# MAGIC * majority?
# MAGIC * greater than 75%?
# MAGIC * combination?

# COMMAND ----------

# DBTITLE 1,Voting by individual models to reduce bias and errors of individual models
@udf('integer')
def ensembleAll(decision_tree, logistic_regression, random_forest, light_gbm, xgboost):
  votes = decision_tree +logistic_regression + random_forest + light_gbm + xgboost
  if votes >= 4:
    return 1
  else:
    return 0

@udf('integer')
def ensembleTop4(decision_tree, random_forest, light_gbm, xgboost):
  votes = decision_tree + random_forest + light_gbm + xgboost
  if votes >= 3:
    return 1
  else:
    return 0
  
@udf('integer')
def ensembleTop3(random_forest, light_gbm, xgboost):
  votes = random_forest + light_gbm + xgboost
  if votes >= 2:
    return 1
  else:
    return 0
  
@udf('integer')
def ensembleTop2(light_gbm, xgboost):
  votes = light_gbm + xgboost
  if votes >= 1:
    return 1
  else:
    return 0

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Comparing the automl generated best model with the various Ensembles

# COMMAND ----------

# DBTITLE 1,Ensemble Score
model_type='Ensemble'
test_df = test_df.withColumn('FullEnsemble', ensembleAll('DecisionTree','LogisticRegression','RandomForest','LGBM', 'XGB'))
test_df = test_df.withColumn('ensembleTop4', ensembleTop4('DecisionTree','RandomForest','LGBM', 'XGB'))
test_df = test_df.withColumn('ensembleTop3', ensembleTop3('RandomForest','LGBM', 'XGB'))
test_df = test_df.withColumn('ensembleTop2', ensembleTop2('LGBM', 'XGB'))
display(test_df)   

# COMMAND ----------

# DBTITLE 1,Ensemble Metrics
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, classification_report, confusion_matrix


def plotCM(y_pred, model_type):
  labels = [0,1]
  cm = confusion_matrix(y_test, y_pred, labels)
  print(cm)
  fig = plt.figure()
  ax = fig.add_subplot(111)
  cax = ax.matshow(cm)
  plt.title(model_type)
  fig.colorbar(cax)
  ax.set_xticklabels([''] + labels)
  ax.set_yticklabels([''] + labels)
  plt.xlabel('Predicted Label')
  plt.ylabel('True Label')
  plt.show()
  print("F1 Score: ", f1_score(y_test, y_pred, average="macro"))
  print("Precision Score: ", precision_score(y_test, y_pred, average="macro"))
  print("Recall Score: ", recall_score(y_test, y_pred, average="macro")) 

# COMMAND ----------

# DBTITLE 1,Generate Confusion Matrices for the best model and the various ensembles
pdf = test_df.toPandas()

y_pred = pdf["bestModel"]
plotCM(y_pred, 'bestModel"')

y_pred = pdf["FullEnsemble"]
plotCM(y_pred, 'FullEnsemble"')

y_pred = pdf["ensembleTop4"]
plotCM(y_pred, 'ensembleTop4')

y_pred = pdf["ensembleTop3"]
plotCM(y_pred, 'ensembleTop3')

y_pred = pdf["ensembleTop2"]
plotCM(y_pred, 'ensembleTop2')

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLFlow: Ensemble management options
# MAGIC 
# MAGIC Promote to Registry

# COMMAND ----------

# DBTITLE 1,Final List of Models in the Ensemble
model_types = ['DecisionTree', 'RandomForest', 'LGBM', 'XGB']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option#1: Log each model separately

# COMMAND ----------

# DBTITLE 1,1 - Log each model of ensemble separately in registry, promote to staging/production
for model_type in model_types:
  filter_str = "params.classifier LIKE '" + model_type + "%'"
  model_name = model_type
  model = (client.search_runs(experiment_ids=experiment_id, filter_string=filter_str, order_by=["metrics.val_f1_score DESC"]))[0]
  best_runId = model.info.run_uuid
  model_uri = f"runs:/{best_runId}/model"
  model_details = mlflow.register_model(model_uri, model_name)

# The mode may be promoted to staging or production
model_stage = 'Production'
for model_name in model_types:
    client.transition_model_version_stage(model_name, 1, stage=model_stage)

# COMMAND ----------

# DBTITLE 1,2 - Load all registered models and use ensemble to predict
def ensemble_inference(df):
  from mlflow.tracking import MlflowClient
  client = mlflow.tracking.MlflowClient()
  
  #load all 4 models : latest from production
  for model_name in model_types:
    model_version = client.get_latest_versions(model_name, stages=[model_stage])[0].version
    model_uri = "models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage=model_stage)
    predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="integer")
  
  #score
    df = df.withColumn(model_name, predict_udf())
  
  #udf
  ensemble_predict_df = df.withColumn('prediction', ensembleTop4('DecisionTree','RandomForest','LGBM', 'XGB'))
  display(ensemble_predict_df)
  return ensemble_predict_df

# COMMAND ----------

# DBTITLE 1,3 - Score new data
ensemble_predict_df = ensemble_inference(new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option#2: Log single ensemble model 
# MAGIC 
# MAGIC ##### Use pyfunc to treat the ensemble as a single model
# MAGIC * register the model
# MAGIC * inference by referencing the ensemble
# MAGIC * https://databricks.com/notebooks/dff/01_dff_model.html

# COMMAND ----------

# DBTITLE 1,1 - Load individual models
filter_str = "params.classifier LIKE 'DecisionTree%'"
model = (client.search_runs(experiment_ids=experiment_id, filter_string=filter_str, order_by=["metrics.val_f1_score DESC"]))[0]
best_runId = model.info.run_uuid
DecisionTree_model_uri = f"runs:/{best_runId}/model"    
DecisionTree_model = mlflow.sklearn.load_model(DecisionTree_model_uri) 


filter_str = "params.classifier LIKE 'RandomForest%'"
model = (client.search_runs(experiment_ids=experiment_id, filter_string=filter_str, order_by=["metrics.val_f1_score DESC"]))[0]
best_runId = model.info.run_uuid
RandomForest_model_uri = f"runs:/{best_runId}/model"    
RandomForest_model = mlflow.sklearn.load_model(RandomForest_model_uri)  


filter_str = "params.classifier LIKE 'LGBM%'"
model = (client.search_runs(experiment_ids=experiment_id, filter_string=filter_str, order_by=["metrics.val_f1_score DESC"]))[0]
best_runId = model.info.run_uuid
LGBM_model_uri = f"runs:/{best_runId}/model"    
LGBM_model = mlflow.sklearn.load_model(LGBM_model_uri)

filter_str = "params.classifier LIKE 'XGB%'"
model = (client.search_runs(experiment_ids=experiment_id, filter_string=filter_str, order_by=["metrics.val_f1_score DESC"]))[0]
best_runId = model.info.run_uuid
XGB_model_uri = f"runs:/{best_runId}/model"    
XGB_model = mlflow.sklearn.load_model(XGB_model_uri) 

# COMMAND ----------

# DBTITLE 1,2 - create an ensemble pyfunc model by passing each individual model to it and log in tracking server
import functools

class Ensemble(mlflow.pyfunc.PythonModel):
  def __init__(self, DecisionTree, RandomForest, LGBM, XGB):
    self.DecisionTree = DecisionTree
    self.RandomForest = RandomForest
    self.LGBM = LGBM
    self.XGB = XGB
  
  def load_context(self, context):
    import numpy as np
    import pandas as pd

    return

  #  Helper function to decide based on the number of models provided
  def decide(self, votes, num_scores):
    # The output and return logic will need to change for multiclass as you need to return 0-N as result.
    if votes >= int(num_scores/2) + 1:
      return 1
    else:
      return 0

  #  Scores is a list of series of predictions from the other classifiers
  def ensembleTopN(self, *scores):    
    # This line needs to change for creating votes for multi class.  
    votes = functools.reduce(lambda x, y: x+y, scores)
    num_scores = len(scores)
    decide_with_num_scores = functools.partial(self.decide, num_scores=num_scores)
    decide_vec = np.vectorize(decide_with_num_scores)
    # Since this is a binary clasification return will be 0 or 1    
    return decide_vec(votes)

    
  # Input is pandas dataframe or series       
  def predict(self, context, model_input):
    dt = self.DecisionTree.predict(model_input)
    rf = self.RandomForest.predict(model_input)
    lgbm = self.LGBM.predict(model_input)
    xgb = self.XGB.predict(model_input)
    ensemble = self.ensembleTopN(
      dt,rf,lgbm,xgb
    )
    return pd.DataFrame({
      "DecisionTreePredictions": dt,
      "RandomForestPredictions": rf,
      "LGBMPredictions": lgbm,
      "XGBPredictions": xgb,
      "EnsemblePredeictions": ensemble
    })

with mlflow.start_run(experiment_id=experiment_id) as ensemble_run:
  mlflow.log_param("DecisionTree", DecisionTree_model_uri)
  mlflow.log_param("RandomForest", RandomForest_model_uri)
  mlflow.log_param("LGBM", LGBM_model_uri)
  mlflow.log_param("XGB", XGB_model_uri)
  
  mlflow.pyfunc.log_model("Ensemble", python_model=Ensemble(DecisionTree_model, RandomForest_model, LGBM_model, XGB_model))
  
print(ensemble_run.info.run_uuid)

# COMMAND ----------

# DBTITLE 1,3 - Perform test inference using model details from tracking server
import mlflow
# Generate the run uri for the ensemble model from the previous run
single_ensemble_model = f'runs:/{ensemble_run.info.run_uuid}/Ensemble'
# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(single_ensemble_model)
  

# Predict on a Pandas DataFrame.
import pandas as pd
import numpy as np
loaded_model.predict(X_test)

# COMMAND ----------

# DBTITLE 1,4 - Promote ensemble model to registry
model_name = 'single_ensemble_model'
model_desc = "Combined Ensemble Model which pickles the 'DecisionTree', 'RandomForest', 'LGBM', 'XGB' models."
client = MlflowClient()
client.create_registered_model(model_name)

# Create a new version of the  model under the registered model name
model_uri = "runs:/{}/Ensemble".format(ensemble_run.info.run_uuid)
print(model_uri)
# 4381625721814023
# 0a2d4b07fea642e1b5b216c7519c5d69
artifact_path = f"dbfs:/databricks/mlflow-tracking/{experiment_id}/{ensemble_run.info.run_uuid}/artifacts/Ensemble"
mv = client.create_model_version(model_name, artifact_path, ensemble_run.info.run_id, description=model_desc)
print("Name: {}".format(mv.name))
print("Version: {}".format(mv.version))
print("Description: {}".format(mv.description))
print("Status: {}".format(mv.status))
print("Stage: {}".format(mv.current_stage))


# COMMAND ----------

# DBTITLE 1,5 - Transition Model to Production
client.transition_model_version_stage(
    name=model_name,
    version=1,
    stage="Production"
)

# COMMAND ----------

# DBTITLE 1,6 - Load ensemble from registry to do inference on new data
import mlflow
import functools
single_ensemble_model = 'models:/{}/Production'.format(model_name)
# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(single_ensemble_model)
  

# Predict on a Pandas DataFrame.
import pandas as pd
import numpy as np
loaded_model.predict(X_test)
