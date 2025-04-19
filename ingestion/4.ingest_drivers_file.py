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

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), True), 
                                     StructField("driverRef", StringType(), True),
                                     StructField("number", IntegerType(), True), 
                                     StructField("code", StringType(), False),
                                     StructField("name", name_schema, True),  
                                     StructField("dob", DateType(), True), 
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True) ])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, concat, lit

# COMMAND ----------

drivers_selected_df = drivers_df.drop("url")

# COMMAND ----------

drivers_ingestion_date_df = add_date_fields(drivers_selected_df, v_file_date)

# COMMAND ----------

drivers_final_df = drivers_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")