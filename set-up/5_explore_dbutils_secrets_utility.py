# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope='formula1-datalake-scope', key='formula1-datalake-account-key')

# COMMAND ----------


