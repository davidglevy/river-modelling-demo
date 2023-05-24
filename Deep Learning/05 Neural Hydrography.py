# Databricks notebook source
# MAGIC %pip install neuralhydrology

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download the CAMELS US data set
# MAGIC
# MAGIC Instructions adapted from this tutorial: https://neuralhydrology.readthedocs.io/en/latest/tutorials/data-prerequisites.html#Data-Prerequisites

# COMMAND ----------

from urllib import request
from rivers_common import get_ext_loc
import zipfile

# COMMAND ----------

url = "https://gdex.ucar.edu/dataset/camels/file/basin_timeseries_v1p2_metForcing_obsFlow.zip"
file_name = url.split("/")[-1]
local_file = f"/tmp/{file_name}"

request.urlretrieve("https://gdex.ucar.edu/dataset/camels/file/basin_timeseries_v1p2_metForcing_obsFlow.zip", local_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store the Zip File in ADLS

# COMMAND ----------

# MAGIC %sh
# MAGIC cp -pr /tmp/basin_timeseries_v1p2_metForcing_obsFlow /tmp/CAMELS_US

# COMMAND ----------



# COMMAND ----------

external_location = get_ext_loc()

dbutils.fs.mkdirs(external_location + "/zip")
dbutils.fs.cp("file://" + local_file, external_location + "/zip")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract the Zip File

# COMMAND ----------


local_directory = "/tmp/basin_timeseries_v1p2_metForcing_obsFlow"
with zipfile.ZipFile(local_file, 'r') as zip_ref:
    zip_ref.extractall(local_directory)

# COMMAND ----------

dbutils.fs.mkdirs(external_location + "/basin_mean_forcing")
dbutils.fs.cp("file:///tmp/basin_timeseries_v1p2_metForcing_obsFlow/basin_dataset_public_v1p2/basin_mean_forcing", external_location + "/basin_mean_forcing", True)

# COMMAND ----------

dbutils.fs.mkdirs(external_location + "/basin_metadata")
dbutils.fs.cp("file:///tmp/basin_timeseries_v1p2_metForcing_obsFlow/basin_dataset_public_v1p2/basin_metadata", external_location + "/basin_metadata", True)

# COMMAND ----------

dbutils.fs.mkdirs(external_location + "/elev_bands_forcing")
dbutils.fs.cp("file:///tmp/basin_timeseries_v1p2_metForcing_obsFlow/basin_dataset_public_v1p2/elev_bands_forcing", external_location + "/elev_bands_forcing", True)

# COMMAND ----------

dbutils.fs.mkdirs(external_location + "/hru_forcing")
dbutils.fs.cp("file:///tmp/basin_timeseries_v1p2_metForcing_obsFlow/basin_dataset_public_v1p2/hru_forcing", external_location + "/hru_forcing", True)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /tmp/basin_timeseries_v1p2_metForcing_obsFlow/basin_dataset_public_v1p2 | grep drwx

# COMMAND ----------

# MAGIC %sh
# MAGIC find /tmp/basin_timeseries_v1p2_metForcing_obsFlow/basin_dataset_public_v1p2 | grep nldas | head
