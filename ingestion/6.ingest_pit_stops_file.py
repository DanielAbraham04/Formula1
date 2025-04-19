# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../config/configurations"

# COMMAND ----------

# MAGIC %run "../config/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")

# COMMAND ----------

final_df = add_date_fields(final_df, v_file_date)

# COMMAND ----------

# perform_incremental_load(final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

merge_delta_data(final_df, "f1_processed", "pit_stops", "src.race_id = tgt.race_id and src.driver_id = tgt.driver_id and src.stop = tgt.stop","race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*)
# MAGIC from f1_processed.pit_stops
# MAGIC group by race_id 
# MAGIC order by race_id desc;