package com.study.spark.sql.streaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCountExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local").appName("WordCount").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._
        // lines 是一个无界表, 不断的接受流数据
        // 数据流中的每一行就是这个无界表中的每一行
        val lines: DataFrame = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
        // 将DataFrame转换为Dataset[String]便于将每一行拆分为多个单词
        val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))
        // 对上步拆分后的单词进行分组计数, 定义新的DataFrame
        val wordCounts: DataFrame = words.groupBy("value").count()
        val query = wordCounts.writeStream
            // append 接受流式DataFrame或Dataset新数据并输出, 不包含任何聚合的查询
            // complete 每次更新都将输出流式DataFrame或Dataset所有数据，只能在包含聚合的场景下使用
            // update 每次更新仅输出流式DataFrame和DataSet中更新的行，如果查询中不包含聚合，类似于append模式
            .outputMode("update")
            .format("console")
            .start()
        query.awaitTermination()
    }

}
