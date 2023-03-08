# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *

@dlt.create_table(
  name='msk_admissions_tbl_raw',
  comment="BRONZE TABLE FOR ADMISSIONS DATA FROM AWS MSK",
  table_properties={
    "quality": "bronze"
  }
)


def msk_stream():
  input_schema = StructType(
    [ 
      StructField('admissions_id', StringType()),
      StructField('hospital_id', IntegerType()),
      StructField('patient_id', IntegerType()),
      StructField('Timestamp', TimestampType())
    ]
  )
  topic = "admission"
  kafka_bootstrap_servers_plaintext=dbutils.secrets.get(scope='myscope_aj',key='MSK_kafka_bootstrap_servers_plaintext')
  return (
   spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", topic )
  .option("failOnDataLoss", "false")
  .option("startingOffsets", "earliest" )
  .load()
  .withColumn("admission_json", F.from_json(F.col("value").cast("string"), input_schema))
  .withColumn("msk_event_timestamp",F.col("timestamp"))
  .select("msk_event_timestamp","admission_json.*")
    )
