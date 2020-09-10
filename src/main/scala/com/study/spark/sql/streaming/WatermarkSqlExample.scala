package com.study.spark.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object WatermarkSqlExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("RateSource").getOrCreate()
        spark.sparkContext.setLogLevel("warn")
        var lines = spark.readStream.format("rate")
            // 每秒生成条数
            .option("rowsPerSecond", 10)
            // 多长时间后达到指定速率
            .option("rampUpTime", 0)
            // 生成数据的分区数
            .option("numPartitions", 1)
            .load()
        lines.printSchema()
        lines.withWatermark("timestamp", "10 seconds").createOrReplaceTempView("test")
        lines = lines.sparkSession.sql("select minute(timestamp), sum(value) from test group by minute(timestamp)")
        val query = lines.writeStream.outputMode("update").format("console").trigger(Trigger.ProcessingTime(2)).start()
        query.awaitTermination()
    }

}
