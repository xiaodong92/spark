package com.study.spark

import org.apache.spark.sql.SparkSession

object SchemaInferenceExample {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("SchemaInference").getOrCreate()
        spark.sparkContext.setLogLevel("warn")
        spark.conf.set("spark.sql.streaming.schemaInference", "true")
        val lines = spark.readStream.format("csv")
            .option("header", "true")
            .option("sep", ";")
            .load("file:/Users/lixiaodong/temp/people/")
        val query = lines.writeStream.outputMode("update").format("console").start()
        query.awaitTermination()
    }
}
