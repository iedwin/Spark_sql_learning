package com.boy.spark

import com.tool.LogConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HiveContext的使用
  * --jars传递mysql驱动
  */
object HiveContextApp {

  def main(args: Array[String]): Unit = {
    LogConf.setStreamingLogLevels
    //1) 创建相应的Context
//    val sparkConf = new SparkConf()
    //在测试或者生产中，AppBName和Master我们是通过脚本进行指定
    //sparkConf.setAppName("HiveContextApp").setMaster("local[2]")
    val spark = SparkSession.builder()
      .appName("DatasetApp")
      .master("local[2]")
      .enableHiveSupport
      .getOrCreate()

    val sqlDF = spark.sql("select * from emp")
    sqlDF.show

    //3) 关于资源
    spark.stop()
  }

}
