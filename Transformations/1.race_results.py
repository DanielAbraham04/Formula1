# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../config/configurations"

# COMMAND ----------

# MAGIC %run "../config/common_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

races_df = spark.read.format('delta').load(f"{processed_folder_path}/races") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("name","circuit_name") \
.withColumnRenamed("location","circuit_location") \
.withColumnRenamed("country","circuit_country")

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("nationality","driver_nationality") \
.withColumnRenamed("number","driver_number")

# COMMAND ----------

constructors_df = spark.read.format('delta').load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name","constructor_name")

# COMMAND ----------

results_df = spark.read.format('delta').load(f"{processed_folder_path}/results") \
.withColumnRenamed("time","race_time") \
.withColumnRenamed("race_id","results_race_id") \
.filter(f"file_date = '{v_file_date}'" ) \
.withColumnRenamed("file_date","results_file_date")

# COMMAND ----------

rc_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")

# COMMAND ----------

df = results_df.join(rc_df, results_df.results_race_id == rc_df.race_id, "inner") \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")

# COMMAND ----------

final_df = df.select(col("race_id"),col("results_file_date"), col("driver_nationality"),col("driver_name"), col("driver_number"), col("constructor_name"), col("grid"), col("fastest_lap"), col("race_time"), col("points"), col("position"), col("race_name"),  col("race_year"), col("race_date"), col("circuit_location")) \
.withColumn("created_date",current_timestamp()) \
.withColumnRenamed("results_file_date","file_date")

# COMMAND ----------

# perform_incremental_load(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_delta_data(final_df, "f1_presentation", "race_results", "src.driver_name = tgt.driver_name and src.race_id = tgt.race_id","race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC from f1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*)
# MAGIC from f1_presentation.race_results
# MAGIC group by race_id 
# MAGIC order by race_id desc;