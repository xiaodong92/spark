package com.study.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object FileSourceExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("FileStream").getOrCreate()
        spark.sparkContext.setLogLevel("warn")
        val schema = new StructType().add("name", "string").add("age", "int").add("job", "string")
        val lines = spark.readStream.format("csv")
            .schema(schema)
            .option("sep", ";")
            .load("file:/Users/lixiaodong/temp/people/")
        val query = lines.writeStream.outputMode("update").format("console").start()
        query.awaitTermination()
    }

}
