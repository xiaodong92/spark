package com.study.spark.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object DataFrameExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("DataFrame").getOrCreate()
        spark.sparkContext.setLogLevel("warn")
        val schema = new StructType().add("name", "string").add("age", "int").add("job", "string")
        val data = spark.readStream.format("csv")
            .schema(schema)
            .option("sep", ";")
            .load("/Users/lixiaodong/temp/people")
        data.createOrReplaceTempView("people")
//        val cnt = spark.sql("select age, count(1) count from people group by age")
        val cnt = data.groupBy("age").count()
        val query = cnt.writeStream
            .outputMode("update")
            .format("console")
            .start()

        query.awaitTermination()
    }

}
