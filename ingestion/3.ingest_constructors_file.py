# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../config/configurations"

# COMMAND ----------

# MAGIC %run "../config/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema). \
json(f"{raw_folder_path}/{v_file_date}/constructors.json"
                             )

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, lit

# COMMAND ----------

constructors_selected_df = constructors_df.drop("url")

# COMMAND ----------

constructors_ingestion_date_df = add_date_fields(constructors_selected_df, v_file_date)

# COMMAND ----------

constructors_final_df = constructors_ingestion_date_df.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

constructors_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")