-- Databricks notebook source
select * from f1_raw.circuits;

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

select * from f1_processed.circuits;

-- COMMAND ----------

select * from f1_processed.races;

-- COMMAND ----------

select * from f1_processed.drivers;

-- COMMAND ----------

select * from f1_processed.constructors;

-- COMMAND ----------

select * from f1_processed.lap_times;

-- COMMAND ----------

select * from f1_processed.results;

-- COMMAND ----------

select * from f1_processed.pit_stops;

-- COMMAND ----------

select * from f1_processed.qualifying;

-- COMMAND ----------

select * from f1_presentation.race_results

-- COMMAND ----------

select * from f1_presentation.driver_standings

-- COMMAND ----------

select * from f1_presentation.constructor_standings