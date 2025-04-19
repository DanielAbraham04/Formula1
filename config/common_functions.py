# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

def add_date_fields(input_df, file_date):
  output_df = input_df.withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(file_date))
  return output_df

# COMMAND ----------

def put_partition_col_at_last(input_df, partition_col):
    lcol = input_df.schema.names
    if partition_col in lcol:
        lcol.remove(partition_col)
    df = input_df.select(lcol + [partition_col])
    return df


# COMMAND ----------

def set_to_dynamic():
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

def perform_incremental_load(input_df, database_name, table_name, partition_col):
    df = put_partition_col_at_last(input_df, partition_col)
    set_to_dynamic()
    if (spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}")):
        df.write.mode('overwrite').insertInto(f"{database_name}.{table_name}")
    else:
        df.write.mode('overwrite').partitionBy(partition_col).format('parquet') \
        .saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

from delta import *
def merge_delta_data(input_df, database_name, table_name, merge_condition, partition_col):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    folder_name = database_name[3:]
    if (spark._jsparkSession.catalog().tableExists(f"{database_name}.{table_name}")):
        delta_table = DeltaTable.forPath(spark, f"abfss://{folder_name}@formula1dlu.dfs.core.windows.net/{table_name}")
        delta_table.alias("tgt").merge(input_df.alias("src"), merge_condition) \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        input_df.write.mode('overwrite').partitionBy(partition_col).format('delta') \
        .saveAsTable(f"{database_name}.{table_name}")