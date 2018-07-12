package com.boy.spark

import com.tool.LogConf
import org.apache.spark.sql.SparkSession

/**
  * DataFrame中的操作
  */
object DataFrameCase {
  def main(args: Array[String]): Unit = {

    LogConf.setStreamingLogLevels

    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    val rdd = spark.sparkContext.textFile("src/main/data/student.data")

    import spark.implicits._
    val studentDS = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDS()

    studentDS.show(30, false)
    //前10条
    studentDS.take(10).foreach(println)
    //第一条记录
    studentDS.first()
    //前三条
    studentDS.head(3)
    //指定的列
    studentDS.select("name", "email").show(30, false)
    //过滤name=''
    studentDS.filter("name='' OR name='NULL'").show()

    //name以M开头的人
    studentDS.filter("SUBSTR(name,0,1)='M'").show()
    //查询内置函数
    spark.sql("show functions").show(1000)

    // //name以M开头的人
    studentDS.filter("substring(name,0,1)='M'").show()

    //排序
    studentDS.sort(studentDS("name")).show()
    studentDS.sort(studentDS("name").desc).show()
    studentDS.sort("name", "id").show()
    studentDS.sort(studentDS("name").asc, studentDS("id").desc).show()

    //重命名
    studentDS.select(studentDS("name").as("student_name")).show()

    //join操作
    val studentDS2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    studentDS.join(studentDS2, studentDS.col("id") === studentDS2.col("id")).show()

    studentDS.join(studentDS2, studentDS.col("id") === studentDS2.col("id"), "left_outer").select(studentDS("name")).show()

    spark.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)

}
