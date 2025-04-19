# Databricks notebook source
formula1dlu_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlu-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlu.dfs.core.windows.net",
    formula1dlu_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlu.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlu.dfs.core.windows.net/circuits.csv"))