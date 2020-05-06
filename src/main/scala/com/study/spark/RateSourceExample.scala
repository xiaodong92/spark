package com.study.spark

import org.apache.spark.sql.SparkSession

object RateSourceExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("RateSource").getOrCreate()
        spark.sparkContext.setLogLevel("warn")
        val lines = spark.readStream.format("rate")
            // 每秒生成条数
            .option("rowsPerSecond", 10)
            // 多长时间后达到指定速率
            .option("rampUpTime", 0)
            // 生成数据的分区数
            .option("numPartitions", 1)
            .load()
        val query = lines.writeStream.outputMode("update").format("console").start()
        query.awaitTermination()
    }

}
