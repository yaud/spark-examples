package org.github.yaud.spark.examples.structured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime

object KafkaDump extends App {
  case class KafkaMessage(val key: String, val value: Array[Byte],
    val topic: String, val partition: Int, val offset: Long, 
    val year: Int, val month: Int, val day: Int)

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  val messages = spark.readStream
    .format("kafka")
    .option("subscribe", "test-topic")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingOffsets", "earliest")
    .load()
    .withColumn("year", year($"timestamp"))
    .withColumn("month", month($"timestamp"))
    .withColumn("day", dayofmonth($"timestamp"))
    .as[KafkaMessage]

  import scala.concurrent.duration._
  val query = messages.writeStream
    .partitionBy("key", "year", "month", "day")
    .format("parquet")
    .option("checkpointLocation", "/tmp/spark/kafka-dump/checkpoint")
    .trigger(ProcessingTime(10.minutes))
    .start("/tmp/spark/kafka-dump/output/")
  query.awaitTermination()
}
