package com.jitkasem

import abris.avro.read.confluent.SchemaManager
import abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object KafkaSparkCosmosDB {

  private val RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]"
  private val APPLICATION_NAME = "SparkStructuredStreaming"

  def main(args: Array[String]): Unit = {

    val kafkaUrl = "ip-172-31-17-31.ec2.internal:9092"

    val schemaRegistryURL = "http://ip-172-31-17-31.ec2.internal:8081"

    val topic = "confluent-in-prices"

    val schemaRegistryConfs = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL          -> schemaRegistryURL,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC        -> topic,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_ID              -> "21" // set to if you want the latest schema version to used
    )

    val conf = new SparkConf()
                  .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                  .setAppName(APPLICATION_NAME)

    val spark = SparkSession.builder.config(conf).getOrCreate()

    import abris.avro.AvroSerDe._

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-17-31.ec2.internal:9092")
      .option("subscribe", topic)
      .fromConfluentAvro("value", Some("/home/ubuntu/poc_streaming_twitter_to_kafka_to_spark_to_hdfs/scala_spark_to_hdfs/src/main/scala/com/jitkasem/confluent-in-prices-value.avsc"), None)(RETAIN_SELECTED_COLUMN_ONLY)


    val dss = df.writeStream.format("console").start().awaitTermination()

  }


}
