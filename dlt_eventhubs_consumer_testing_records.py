# Databricks notebook source
# DBTITLE 1,Imports
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

# COMMAND ----------

EH_CONN_STR=dbutils.secrets.get(scope='myscope_aj',key='testing_records_stream_EH_CONN_STR')

# COMMAND ----------

# DBTITLE 1,Connection Details
EH_NAMESPACE = "akashjaiswalev"
EH_KAFKA_TOPIC = "testing_records_stream"
EH_BOOTSTRAP_SERVERS = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
EH_SASL_WRITE = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";"
EH_SASL_READ = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";"

# COMMAND ----------

# DBTITLE 1,Config for reading from Kafka surface of Event Hub Stream
topic_name = EH_KAFKA_TOPIC
eh_namespace_name = EH_NAMESPACE
eh_sasl = EH_SASL_READ
bootstrap_servers = EH_BOOTSTRAP_SERVERS
kafka_options = {
 "kafka.bootstrap.servers": bootstrap_servers,
 "kafka.sasl.mechanism": "PLAIN",
 "kafka.security.protocol": "SASL_SSL",
 "kafka.request.timeout.ms": "60000",
 "kafka.session.timeout.ms": "30000",
 "startingOffsets": "earliest",
 "kafka.sasl.jaas.config": eh_sasl,
 "subscribe": topic_name,
}

# COMMAND ----------

input_schema = StructType(
    [
        StructField("testing_record_id", StringType(), True),
        StructField("hospital_id", IntegerType(), True),
        StructField("patient_id", IntegerType(), True),
        StructField("is_positive", StringType(), True),
        StructField("Timestamp", TimestampType(), True),
    ]
)

# COMMAND ----------

# DBTITLE 1,Delta Table : testing_records_raw
@dlt.table(name = "eventhubs_testing_records_tbl_raw",
          comment = "Reads raw messages from event hub stream for testing records of patients")
def read_eventhub():
  return (spark.readStream
              .format("kafka")
              .options(**kafka_options)
              .load()
              .withColumn("testing_record_text", F.col("value").cast("string"))
              .withColumn("testing_record_json", F.from_json("testing_record_text", input_schema))
              .select("testing_record_json.*", "testing_record_text")
            )
            
