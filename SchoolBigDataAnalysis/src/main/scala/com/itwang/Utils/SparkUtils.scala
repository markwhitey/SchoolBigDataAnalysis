package com.itwang.Utils

import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * @ProjectName: SchoolBigDataAnalysis 
  * @Package: com.itwang.Utils
  * @ClassName: SparkUtils
  * @Author: hadoop
  * @Description:
  * @Date: 2019-04-01 20:20
  * @Version: 1.0
  */
class SparkUtils {
  def init(): SparkSession = {
    //hive warehouse在hdfs上的存储目录
    val warehouse = "/user/hive/warehouse"
    //创建SparkSession
    val spark = SparkSession.builder()
      .appName("Spark_Hive_Example")
      //.master("spark://master:7077")
      .master("local[3]")
      .config("spark.sql.warehouse.dir", warehouse) //指定spark的warehouse
      .enableHiveSupport()
      .getOrCreate()
    //获取SparkContext
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark
  }

  def getProvince(): Unit ={
    val spark = init()
    val sc = spark.sparkContext
    val data = sc.textFile("src/main/resources/province.txt")
    val rddData = data.map(x => {
      val s = x.split("\t")
      Province(s(0).toInt,s(1))
    })
    import spark.implicits._
    val dataFrame = rddData.toDF()
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    //将数据追加到数据库
    dataFrame.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/schooldata", "province", prop)
    spark.stop()
  }
}
object SparkUtils{
  def main(args: Array[String]): Unit = {
    val sparkUtils = new SparkUtils
    sparkUtils.getProvince()
  }
}
case class Province(id: Integer, province: String)
