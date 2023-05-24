# Databricks notebook source
# MAGIC %sql
# MAGIC USE demo.rivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE water_level_silver
# MAGIC AS SELECT place, `date`, water_level
# MAGIC FROM raw_river_data
# MAGIC WHERE water_level IS NOT NULL; 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE water_level_two_week_silver
# MAGIC AS
# MAGIC SELECT r0.place, r0.date, r0.water_level,
# MAGIC r1.water_level AS water_level_1, 
# MAGIC r2.water_level AS water_level_2, 
# MAGIC r3.water_level AS water_level_3, 
# MAGIC r4.water_level AS water_level_4, 
# MAGIC r5.water_level AS water_level_5, 
# MAGIC r6.water_level AS water_level_6, 
# MAGIC r7.water_level AS water_level_7, 
# MAGIC r8.water_level AS water_level_8, 
# MAGIC r9.water_level AS water_level_9, 
# MAGIC r10.water_level AS water_level_10, 
# MAGIC r11.water_level AS water_level_11, 
# MAGIC r12.water_level AS water_level_12, 
# MAGIC r13.water_level AS water_level_13, 
# MAGIC r14.water_level AS water_level_14 
# MAGIC FROM water_level_silver AS r0
# MAGIC INNER JOIN water_level_silver AS r1 ON (r0.date = r1.date + 1 AND r0.place = r1.place)
# MAGIC INNER JOIN water_level_silver AS r2 ON (r0.date = r2.date + 2 AND r0.place = r2.place)
# MAGIC INNER JOIN water_level_silver AS r3 ON (r0.date = r3.date + 3 AND r0.place = r3.place)
# MAGIC INNER JOIN water_level_silver AS r4 ON (r0.date = r4.date + 4 AND r0.place = r4.place)
# MAGIC INNER JOIN water_level_silver AS r5 ON (r0.date = r5.date + 5 AND r0.place = r5.place)
# MAGIC INNER JOIN water_level_silver AS r6 ON (r0.date = r6.date + 6 AND r0.place = r6.place)
# MAGIC INNER JOIN water_level_silver AS r7 ON (r0.date = r7.date + 7 AND r0.place = r7.place)
# MAGIC INNER JOIN water_level_silver AS r8 ON (r0.date = r8.date + 8 AND r0.place = r8.place)
# MAGIC INNER JOIN water_level_silver AS r9 ON (r0.date = r9.date + 9 AND r0.place = r9.place)
# MAGIC INNER JOIN water_level_silver AS r10 ON (r0.date = r10.date + 10 AND r0.place = r10.place)
# MAGIC INNER JOIN water_level_silver AS r11 ON (r0.date = r11.date + 11 AND r0.place = r11.place)
# MAGIC INNER JOIN water_level_silver AS r12 ON (r0.date = r12.date + 12 AND r0.place = r12.place)
# MAGIC INNER JOIN water_level_silver AS r13 ON (r0.date = r13.date + 13 AND r0.place = r13.place)
# MAGIC INNER JOIN water_level_silver AS r14 ON (r0.date = r14.date + 14 AND r0.place = r14.place);

# COMMAND ----------

places_rows = spark.sql("SELECT DISTINCT(place) FROM water_level_silver").collect()
places = [x['place'] for x in places_rows]

# COMMAND ----------

from databricks.feature_store.client import FeatureStoreClient
from databricks.feature_store.entities.feature_lookup import FeatureLookup
 
fs = FeatureStoreClient()


# COMMAND ----------

# Creates a time-series feature table for the temperature sensor data using the room as a primary key and the time as the timestamp key. 
water_levels_df = spark.sql("SELECT * FROM water_level_two_week_silver")
#fs.create_table(
#    f"water_level_features",
#    primary_keys=["place"],
#    timestamp_keys=["date"],
#    df=water_levels_df,
#    description="Readings from water level",
#)

# COMMAND ----------

for place in places:
    print(place)
    place_table_name = "water_level_feature_" + place.replace(" ", "_")
    spark.sql(f"CREATE OR REPLACE TABLE {place_table_name} AS SELECT * FROM water_level_two_week_silver WHERE place = '{place}'")
