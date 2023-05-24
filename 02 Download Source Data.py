# Databricks notebook source
# MAGIC %sql
# MAGIC USE demo.rivers;

# COMMAND ----------

from rivers_common import get_ext_loc
from urllib import request

locations = [
    {"place": "Albury", "url": "https://riverdata.mdba.gov.au/sites/default/files/liveriverdata/csv/409001_historical.csv"},
    {"place":"Balranald", "url":"https://riverdata.mdba.gov.au/sites/default/files/liveriverdata/csv/410130_historical.csv"},
    {"place":"Corowa", "url": "https://riverdata.mdba.gov.au/sites/default/files/liveriverdata/csv/409002_historical.csv"}
]


external_location = get_ext_loc()

def build_storage_loc(place):
    return external_location + "/" + place.replace(" ", "-").lower()

def trim_csv_header(source_file, dest_file):
    lines = []
    # read file
    with open(source_file, 'r') as fp:
        # read an store all lines into list
        lines = fp.readlines()[4:]

    # Write file
    with open(dest_file, 'w') as fp:
        fp.writelines(lines)

for loc in locations:
    file_name_base = loc["url"].split("/")[-1].replace(".csv", "")
    print(file_name_base)

    file_name_pre = "/tmp/" + file_name_base + "_1.csv"
    request.urlretrieve(loc["url"], file_name_pre)

    file_name_post = "/tmp/" + file_name_base + "_2.csv"
    trim_csv_header(file_name_pre, file_name_post)

    storage_loc = build_storage_loc(loc["place"])
    dbutils.fs.mkdirs(storage_loc)
    dbutils.fs.cp("file://" + file_name_post, storage_loc + "/" + file_name_base + ".csv")



# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM raw_river_data;

# COMMAND ----------

from pyspark.sql.functions import to_date, split, lit

for loc in locations:
    storage_loc = build_storage_loc(loc["place"])
    schema = "date_txt string, water_level float, cond_at_25_degc float, water_temp_degc float"
    df = spark.read.option("header",False).schema(schema).format("csv").load(storage_loc)
    df = df.withColumn("date", to_date(split("date_txt"," ")[1], "dd/MM/yyyy"))
    table_name = loc["place"].replace(" ", "-").lower()
    df = df.withColumn("place", lit(table_name))

    df = df.drop("date_txt")


    df.write.mode("append").saveAsTable("raw_river_data")

