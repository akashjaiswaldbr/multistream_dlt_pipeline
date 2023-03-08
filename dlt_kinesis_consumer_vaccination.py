# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import *


my_stream_name = 'test_vaccination_data_1' 
kinesisRegion = 'us-west-2'

@dlt.create_table(
  name='kinesis_vaccination_tbl_raw',
  comment="BRONZE TABLE FOR VACCINATION DATA FROM KINESIS",
  table_properties={
    "quality": "bronze"
  }
)
def kinesis_stream():
  input_schema = StructType(
    [ 
      StructField('hospital_id', IntegerType()),
      StructField('patient_id', IntegerType()),
      StructField('vaccination_type', StringType()),
      StructField('Timestamp', TimestampType())
    ]
  )

  return (
    spark
    .readStream
    .format("kinesis")
    .option("streamName", my_stream_name)
    .option("initialPosition", "earliest")
    .option("region", kinesisRegion)
    .load()
    .withColumn('value',F.from_json(F.col("data").cast("string"), input_schema))                
    .withColumn('key',F.col('partitionKey').cast("string"))
    .select('key','value.hospital_id','value.patient_id','value.vaccination_type','value.Timestamp')
  )

