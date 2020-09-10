package com.study.spark.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object PartitionExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("Partition").getOrCreate()
        spark.sparkContext.setLogLevel("warn")
        val schema = new StructType().add("name", "string").add("age", "int").add("job", "string")
        val lines = spark.readStream.format("csv")
            .option("sep", ";")
            .schema(schema)
            .load("file:/Users/lixiaodong/temp/partition/")
        val query = lines.writeStream.outputMode("update").format("console").start()
        query.awaitTermination()
    }

}
