package com.study.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ResetKafkaOffset {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("ResetKafkaOffset").setMaster("local")
        val spark = SparkSessionSingleton.getInstance(conf)
        val stream = KafkaUtils.createDirectStream[String, String](
            streamingContext,
            PreferConsistent,
            Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
        )
    }

}


/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
        if (instance == null) {
            instance = SparkSession
                .builder
                .config(sparkConf)
                .getOrCreate()
        }
        instance
    }
}