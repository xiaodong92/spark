package com.study.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate()
        val streamContext = new StreamingContext(spark.sparkContext, Seconds(5))
        val words = streamContext.socketTextStream("localhost", 9999)
            .flatMap(_.split(" "))
            .map((_, 1))
        val wordCounts = words.reduceByKey(_ + _)
        wordCounts.print()
        streamContext.start()
        streamContext.awaitTermination()
    }

}
