package com.itwang.DataProcessing

import java.util.Properties

import com.itwang.Utils.SparkUtils
import org.apache.spark.sql.SparkSession

/**
  * @ProjectName: SchoolBigDataAnalysis 
  * @Package: com.itwang.DataProcessing
  * @ClassName: ConsumerProcessing
  * @Author: hadoop
  * @Description:
  * @Date: 2019-03-31 15:35
  * @Version: 1.0
  */
class ConsumerProcessing {
  //创建hive表并加载数据到hvie中
  def loadData(): Unit = {
    val sparkUtils = new SparkUtils
    val spark = sparkUtils.init()
    //操作Hive
    val sourceTable =
      """
        |create table if not exists m_rec_consume(
        |outid string,
        |opdt string,
        |opcount bigint,
        |opfare bigint,
        |dscrp string,
        |termid string)
        |row format delimited fields terminated by '\t'
      """.stripMargin
    spark.sql(sourceTable)
    //将数据加载到分区目录上
    spark.sql("load data inpath '/jikewang/2015consume.txt' into table m_rec_consume")
    spark.sql("load data inpath '/jikewang/2015consume2.txt' into table m_rec_consume")
    val frame = spark.sql("select * from m_rec_consume where outid = '201508040134' limit 10")
    frame.collect().foreach(x => {
      print(x)
    })
    spark.stop()
  }

  //将一卡通消费数据进行汇总获取详细消费数据
  def getDetailConsume(): Unit = {
    val sparkUtils = new SparkUtils
    val spark = sparkUtils.init()
    spark.udf.register("date_to_info", (dscrp: String, dataStr: String) => {
      if ("餐费支出".equals(dscrp)) {
        val currHour = Integer.parseInt(dataStr.substring(11, 13))
        if (currHour >= 5 && currHour < 10) {
          "breakfast"
        }
        else if (currHour >= 10 && currHour <= 15) {
          "lunch"
        }
        else if (currHour > 15 && currHour <= 22) {
          "dinner"
        } else {
          "无规律就餐"
        }
      }
      else if ("用水支出".equals(dscrp)) {
        "用水支出"
      }
      else if ("商场购物".equals(dscrp)) {
        "商场购物"
      }
      else if ("购冷水支出".equals(dscrp)) {
        "用水支出"
      }
      else if ("购热水支出".equals(dscrp)) {
        "用水支出"
      }
      else if ("淋浴支出".equals(dscrp)) {
        "淋浴支出"
      }
      else if ("医疗支出".equals(dscrp)) {
        "医疗支出"
      }
      else {
        "其他"
      }
    })
    //创建需要处理的数据的临时一卡通消费数据视图consume_temp_view
    val sourceView = spark.sql("select * from  m_rec_consume")
    sourceView.createOrReplaceTempView("consume_temp_view")
    //对学生消费按照就餐时间分类
    val process_food = spark.sql("select outid,opdt,opcount,opfare,date_to_info(dscrp,opdt) as dscrp from consume_temp_view where dscrp = '餐费支出'")
    process_food.createOrReplaceTempView("temp_food_process")
    val process_else = spark.sql("select outid,opdt,opcount,opfare,date_to_info(dscrp,opdt) as dscrp from consume_temp_view where dscrp != '餐费支出'")
    process_else.createOrReplaceTempView("temp_else_process")
    //然后按照分组对学生的每次就餐可能有多次刷卡，对其进行统计
    //因为对于学生就餐，可能早、中晚会有多个窗口多个时间，所以按照天分组，每天最多有三次就餐
    val consume_food_result = spark.sql("select outid,min(opdt) as opdt,sum(opfare) as opfare,dscrp from temp_food_process group by outid,to_date(opdt),dscrp")

    //对于学生医疗、用水、洗浴、购物可能一天会有多次消费,特别是用水，下面按照小时进行汇总
    val consume_else_result = spark.sql("select outid,min(opdt) as opdt,sum(opfare) as opfare,dscrp from temp_else_process group by outid,substring(opdt, 0, 13),dscrp")
    //对上面的小汇总进行合并
    consume_food_result.write.saveAsTable("consume_detail_result")
    consume_else_result.write.insertInto("consume_detail_result")
    spark.stop()
  }

  //对汇总的详细数据进行分组统计，获取早餐、午餐、晚餐、用水、商场购物的总消费额和总频次
  def getStatisticsData(): Unit = {
    val sparkUtils = new SparkUtils
    val spark = sparkUtils.init()
    //将单位从分转化为元
    val result = spark.sql("select outid, dscrp, bround(sum(opfare)/100, 2) as total, count(*) as frequency from consume_detail_result group by outid,dscrp")
    result.write.saveAsTable("consume_student_result")
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "hadoop")
    //将数据追加到数据库
    result.write.mode("append").jdbc("jdbc:mysql://39.105.128.239:3306/wang", "consume_student_result", prop)

    //可以根据是上面计算统计的结果进行学生聚类总计得出总消费额，和总消费频次
    val consume_total_result = spark.sql("select outid, sum(total) as total, sum(frequency) as frequency from consume_student_result group by outid")
    consume_total_result.write.saveAsTable("consume_total_result")

    val consume = spark.sql("select * from consume_total_result")
    consume.write.mode("append").jdbc("jdbc:mysql://39.105.128.239:3306/wang", "consume_total_result", prop)
    spark.stop()
  }
}

object ConsumerProcessing {
  def main(args: Array[String]): Unit = {
    val consumerProcessing = new ConsumerProcessing
    consumerProcessing.loadData()
  }
}
