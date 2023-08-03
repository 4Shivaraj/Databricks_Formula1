# Databricks notebook source
formula1_datalake_account_key= dbutils.secrets.get(scope='formula1-datalake-scope', key='formula1-datalake-account-key')

# COMMAND ----------

configs = {
    "fs.azure.account.key.formula1datalake98.dfs.core.windows.net":
    formula1_datalake_account_key
}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1datalake98.dfs.core.windows.net/",
  mount_point = "/mnt/formula1datalake98/demo",
  extra_configs = configs)

# COMMAND ----------


