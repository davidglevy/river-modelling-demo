# Databricks notebook source
# MAGIC %sql
# MAGIC USE demo.rivers;

# COMMAND ----------

import datetime 
from pyspark.sql.functions import to_date

dates = []

for x in range(0,14):
    tod = datetime.datetime.now()
    d = datetime.timedelta(days = x)
    a = tod + d
    row = {"date_txt": a.strftime("%Y-%m-%d")}
    dates.append(row)

df = spark.createDataFrame(dates, "date_txt string")
df = df.withColumn("date", to_date("date_txt", "y-M-d")).drop("date_txt")
display(df)



# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
logged_model = 'runs:/bc99b32b21d8443788124ef6466aff71/model'

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')

# Predict on a Spark DataFrame.
df = df.withColumn('predictions', loaded_model(struct(*map(col, df.columns))))
display(df)

df.write.mode("overwrite").saveAsTable("albury_river_predictions")
