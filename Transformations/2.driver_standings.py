# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../config/configurations"

# COMMAND ----------

# MAGIC %run "../config/common_functions"

# COMMAND ----------

race_results_list = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") \
.select('race_year') \
.distinct() \
.collect()

# COMMAND ----------

race_years = [i.race_year for i in race_results_list]

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, rank, desc
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_years))

# COMMAND ----------

driver_standings_df = race_results_df \
.groupBy('driver_nationality','driver_name','race_year') \
.agg(sum('points').alias('total_points'),count(when(col('position') == 1, True)).alias('wins'))


# COMMAND ----------

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
ranked_driver_standings_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

# perform_incremental_load(ranked_driver_standings_df,"f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

merge_delta_data(ranked_driver_standings_df, "f1_presentation", "driver_standings", "src.driver_name = tgt.driver_name and src.race_year = tgt.race_year","race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC from f1_presentation.driver_standings
# MAGIC order by race_year desc;