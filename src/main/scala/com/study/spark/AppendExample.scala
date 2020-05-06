package com.study.spark

import org.apache.spark.sql.SparkSession

object AppendExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("AppendMain").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
        val query = lines.writeStream.outputMode("update").format("console").start()
        query.awaitTermination()
    }

}
