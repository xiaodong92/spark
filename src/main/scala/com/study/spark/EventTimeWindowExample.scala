package com.study.spark

import java.sql.Timestamp

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EventTimeWindowExample {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder().master("local").appName("EventTimeWindow").getOrCreate()
        spark.sparkContext.setLogLevel("warn")
        val lines = spark.readStream.format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
        import spark.implicits._
        val wordCounts = lines.as[String]
            .map(line => {
                val arr = line.split(";")
                (arr(0), timeParse(arr(1)))
            }).toDF("word", "timestamp")
            .as[(String, Timestamp)]
            .flatMap(line => {
                line._1.split(" ").map(word => (word, line._2))
            })
            .toDF("word", "timestamp")
            .withWatermark("timestamp", "10 minutes")
            .groupBy(
                window($"timestamp", "10 minutes"),
                $"word"
            ).count()

        val query = wordCounts.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .start()

        query.awaitTermination()
    }

    private def timeParse(datetime: String): Timestamp = {
        new Timestamp(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(datetime).getTime)
    }

}
