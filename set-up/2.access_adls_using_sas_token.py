# Databricks notebook source
formula1dlu_sas_token = dbutils.secrets.get(scope = "formula1-scope", key = "formula1dlu-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlu.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlu.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlu.dfs.core.windows.net", formula1dlu_sas_token)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlu.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlu.dfs.core.windows.net/circuits.csv"))