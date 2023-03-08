# Databricks notebook source
import os
import pyspark.sql.functions as F
import dlt 

files_path = "dbfs:/mnt/multistream-dlt-blog/raw_data/claims"
checkpoint_root = 'dbfs:/mnt/multistream-dlt-blog/ss-checkpoints/autoloader'
checkpoint_schema = os.path.join(checkpoint_root, "schemas")
checkpoint_writestream = os.path.join(checkpoint_root, "writestream")

# COMMAND ----------

cloudfiles_options = {
  "header": "true",
  "cloudFiles.format": "json",
  "cloudFiles.useNotifications": "false",
  "cloudFiles.inferColumnTypes": "true",
  "cloudFiles.rescuedDataColumn": "_rescued_data",
  "cloudFiles.schemaHints": "timestamp timestamp",
  "cloudFiles.schemaLocation": checkpoint_root,
  "cloudFiles.schemaEvolutionMode": "addNewColumns"
}

@dlt.table
def autoloader_filestream_claims_tbl_raw ():
  return (spark.readStream
                  .format('cloudFiles')
                  .options(**cloudfiles_options)
                  .load(files_path).withColumn("source_file", F.input_file_name())
                  .withColumn("ingestion_timestamp", F.current_timestamp()))
