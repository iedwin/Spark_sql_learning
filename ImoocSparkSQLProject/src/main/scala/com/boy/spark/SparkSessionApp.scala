package com.boy.spark

import com.tool.LogConf
import org.apache.spark.sql.SparkSession

/**
  * SparkSession的使用
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    LogConf.setStreamingLogLevels
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    val people = spark.read.json("src/main/data/people.json")
    people.show()
    spark.close()
  }
}
