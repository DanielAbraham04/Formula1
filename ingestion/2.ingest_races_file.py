# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../config/configurations"

# COMMAND ----------

# MAGIC %run "../config/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False), 
                                     StructField("year", IntegerType(), True), 
                                     StructField("round", IntegerType(), True), 
                                     StructField("circuitId", IntegerType(), True), 
                                     StructField("name", StringType(), True), 
                                     StructField("date", DateType(), True), 
                                     StructField("time", StringType(), True), 
                                     StructField("url", StringType(), True) ])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", header = True,
                             schema=races_schema
                             )

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), \
                                    col("name"), col("date"), col("time"))

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id")


# COMMAND ----------

races_ingestion_date_df = add_date_fields(races_renamed_df, v_file_date)

# COMMAND ----------

races_date_transformed_df = races_ingestion_date_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(' '),col("time")), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

races_final_df = races_date_transformed_df.select(col("race_id"), col("race_year"), col("round"), col("circuit_id"),
                                                  col("name"), col("ingestion_date"), col("race_timestamp"),col("file_date"))

# COMMAND ----------

races_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")