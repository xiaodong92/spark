package com.study.spark

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

object WatermarkExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("Watermark").getOrCreate()

        val lines = spark.readStream.format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
        import spark.implicits._
        val wordCounts = lines.as[String]
            .map(line => {
                val arr = line.split(";")
                (arr(0), arr(1).toLong)
            }).toDF("word", "timestamp")
            .as[(String, Timestamp)]
            .flatMap(line => {
                line._1.split(" ").map(word => (word, line._2))
            })
            .toDF("word", "timestamp")
            .groupBy(
                window($"timestamp", "10 minutes", "5 minutes"),
                $"word"
            ).count()
            .select("window.start", "window.end", "word", "count")
            .where("window.start >= '2020-04-28 12:00:00'")
    }

}
