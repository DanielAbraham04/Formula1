# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../config/configurations"

# COMMAND ----------

# MAGIC %run "../config/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType, DateType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False), 
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True), 
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),  
                                     StructField("grid", IntegerType(), True), 
                                     StructField("position", IntegerType(), True),
                                     StructField("positionText", IntegerType(), True),
                                     StructField("positionOrder", IntegerType(), True),
                                     StructField("points", IntegerType(), True),
                                     StructField("laps", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", StringType(), True), 
                                     StructField("fastestLap", IntegerType(), True), 
                                     StructField("rank", IntegerType(), True), 
                                     StructField("fastestLapTime", StringType(), True), 
                                     StructField("fastestLapSpeed", DoubleType(), True), 
                                     StructField("statusId", IntegerType(), True)
                                     ])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_selected_df = results_df.drop("statusId")

# COMMAND ----------

results_final_df = results_selected_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

# COMMAND ----------

results_final_df = add_date_fields(results_final_df, v_file_date)

# COMMAND ----------

results_final_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

merge_delta_data(results_final_df, "f1_processed", "results", "src.result_id = tgt.result_id and src.race_id = tgt.race_id","race_id")

# COMMAND ----------

# race_id_list = results_final_df.select("race_id").distinct().collect()
# for r_id in race_id_list:
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {r_id.race_id})")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*)
# MAGIC from f1_processed.results
# MAGIC group by race_id order by race_id desc;