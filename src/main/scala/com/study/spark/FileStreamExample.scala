package com.study.spark

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStreamExample {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val ssc = new StreamingContext(sparkConf, Seconds(10))
        val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](
            "/Users/lixiaodong/temp/*/*/*/",
            (path: Path) => path.getName.endsWith(".txt"),
            newFilesOnly = false
        )
        lines.map(_._2.toString).print()

        ssc.start()
        ssc.awaitTermination()
    }

}
