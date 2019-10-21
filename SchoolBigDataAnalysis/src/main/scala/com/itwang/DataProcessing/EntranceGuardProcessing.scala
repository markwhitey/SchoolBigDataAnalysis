package com.itwang.DataProcessing

import java.util.Properties

import com.itwang.Utils.SparkUtils

/**
  * @ProjectName: SchoolBigDataAnalysis 
  * @Package: com.itwang.DataProcessing
  * @ClassName: EntranceGuard
  * @Author: hadoop
  * @Description:
  * @Date: 2019-04-08 16:06
  * @Version: 1.0
  */
class EntranceGuardProcessing {
  def loadData(): Unit = {
    val sparkUtils = new SparkUtils
    val spark = sparkUtils.init()
    //操作Hive
    val sourceTable =
      """
        |create table if not exists entranceguard(READLOGID string,
        |READDATE string,
        |XGH string,
        |NAME string,
        |CARDTYPE string,
        |ASSOINFO string,
        |MEMORY string)
        |row format delimited fields terminated by '\t'
      """.stripMargin
    println(sourceTable)
    spark.sql(sourceTable)
    //将数据加载到分区目录上
    spark.sql("load data inpath '/data/tushuguanmenjin.txt' overwrite into table entranceguard")
    val frame = spark.sql("select * from entranceguard where XGH = '201200124105' limit 10")
    frame.collect().foreach(x => {
      print(x)
    })
    spark.stop()
  }
  def getKmeanDataProcess(): Unit ={
    val sparkUtils = new SparkUtils
    val spark = sparkUtils.init()
    //创建需要处理的数据的临时视图bookborrow_view
    //数据预处理操作
    //第一将原始数据中的13,14,15,16,17,18级的数据筛选出来
    val sourceView = spark.sql("select * from entranceguard where XGH like '2013%' or XGH like '2014%' or XGH like '2015%' or XGH like '2016%' or XGH like '2017%' or XGH like '2018%'")
    sourceView.createOrReplaceTempView("entranceguard_view")
    //第二，有的人会没有带卡，会借给朋友耍门禁，需要将这些数据合并为一条数据，这里按照小时合并
    val mergeResult = spark.sql("select XGH as outid,NAME as name,min(READDATE) as readdate from entranceguard group by XGH,NAME,substring(READDATE, 0, 16)")
    //将每个人的进入图书馆数据保存到hive仓库中
    mergeResult.write.saveAsTable("entranceguard_mergeresult")
    //将每个人进行数据合并
    val result = spark.sql("select XGH, count(*) as frequency from entranceguard_mergeResult group by XGH")
    result.write.saveAsTable("entranceguard_kmeanresult")

    //数据保存至Mysql中
    val mergeResult2 = spark.sql("select * from entranceguard_mergeresult")
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    //将数据追加到数据库
    mergeResult2.select("outid","name","readdate").write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/schooldata", "menjin_result", prop)

    //测试
    spark.sql("select * from entranceguard_mergeresult where outid='201508040134' limit 12").show()
    spark.stop()
  }
}
object EntranceGuardProcessing{
  def main(args: Array[String]): Unit = {
    val entranceGuardProcessing = new EntranceGuardProcessing
    //entranceGuardProcessing.loadData()
    entranceGuardProcessing.getKmeanDataProcess()
  }
}
