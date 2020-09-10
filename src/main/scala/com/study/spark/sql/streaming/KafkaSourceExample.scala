package com.study.spark.sql.streaming

import org.apache.spark.sql.SparkSession

object KafkaSourceExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("KafkaSource").getOrCreate()
        spark.sparkContext.setLogLevel("warn")
        val lines = spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "10.227.10.191:9092")
            .option("subscribe", "test")
            .load()
        import spark.implicits._
        val data = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .as[(String, String)].flatMap(_._2.split(" ")).groupBy("value").count()
        val query = data.writeStream.outputMode("update").format("console").start()
        query.awaitTermination()
    }

}
