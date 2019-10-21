package com.itwang.Utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * @ProjectName: SchoolBigDataAnalysis 
  * @Package: com.itwang.Utils
  * @ClassName: MyUDF
  * @Author: hadoop
  * @Description:
  * @Date: 2019-04-01 9:08
  * @Version: 1.0
  */
class MyUDF{

}
object MyUDF{
  def main(args: Array[String]): Unit = {
    val dataStr = "2015-09-12 06:46:03"
    println(dataStr.substring(11, 13))
  }
}
