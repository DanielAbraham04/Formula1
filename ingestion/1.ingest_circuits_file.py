# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../config/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../config/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False), 
                                     StructField("circuitRef", StringType(), True), 
                                     StructField("name", StringType(), True), 
                                     StructField("location", StringType(), True), 
                                     StructField("country", StringType(), True), 
                                     StructField("lat", DoubleType(), True), 
                                     StructField("lng", DoubleType(), True), 
                                     StructField("alt", IntegerType(), True), 
                                     StructField("url", StringType(), True) ])

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header = True, schema=circuits_schema)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), 
                                          col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")

# COMMAND ----------

circuits_final_df = add_date_fields(circuits_renamed_df, v_file_date)

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")