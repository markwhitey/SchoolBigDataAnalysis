package com.itwang.DataProcessing

import java.util.Properties

import com.itwang.Utils.SparkUtils

/**
  * @ProjectName: SchoolBigDataAnalysis 
  * @Package: com.itwang.DataProcessing
  * @ClassName: BookData
  * @Author: 王亚强
  * @Description:
  * @Date: 2019-03-29 15:42
  * @Version: 1.0
  */
class BookDataProcessing {
  def loadData(): Unit = {
    val sparkUtils = new SparkUtils
    val spark = sparkUtils.init()
    //操作Hive
    val sourceTable =
      """
        |create table if not exists bookborrow(XH string,
        |Type string,
        |XBMC string,
        |DWMC string,
        |TSMC string,
        |TSLB string,
        |FLH1 string,
        |JSSJ string)
        |partitioned by (termDate string)
        |row format delimited fields terminated by '\t'
      """.stripMargin
    println(sourceTable)
    spark.sql(sourceTable)
    //将数据加载到分区目录上
    spark.sql("load data inpath '/data/2015-2016-01.txt' overwrite into table bookborrow partition (termDate='2015-2016-01')")
    spark.sql("load data inpath '/data/2015-2016-02.txt' overwrite into table bookborrow partition (termDate='2015-2016-02')")
    spark.sql("load data inpath '/data/2016-2017-01.txt' overwrite into table bookborrow partition (termDate='2016-2017-01')")
    spark.sql("load data inpath '/data/2016-2017-02.txt' overwrite into table bookborrow partition (termDate='2016-2017-02')")
    spark.sql("load data inpath '/data/2017-2018-01.txt' overwrite into table bookborrow partition (termDate='2017-2018-01')")
    spark.sql("load data inpath '/data/2017-2018-02.txt' overwrite into table bookborrow partition (termDate='2017-2018-02')")
    spark.sql("load data inpath '/data/2018-2019-01.txt' overwrite into table bookborrow partition (termDate='2018-2019-01')")
    spark.sql("load data inpath '/data/2018-2019-02.txt' overwrite into table bookborrow partition (termDate='2018-2019-02')")
    val frame = spark.sql("select * from bookborrow where XH = '201200124105'")
    frame.collect().foreach(x => {
      print(x)
    })
    spark.stop()
  }


  //这里的图书借阅数据统计的是截止到2019-01-20日的数据
  def getKmeanDataProcess(): Unit = {
    val sparkUtils = new SparkUtils
    val spark = sparkUtils.init()
    //创建需要处理的数据的临时视图bookborrow_view
    val sourceView = spark.sql("select * from bookborrow where termDate != '2018-2019-02' and (XH like '2013%' or XH like '2014%' or XH like '2015%' or XH like '2016%' or XH like '2017%' or XH like '2018%')")
    sourceView.createOrReplaceTempView("bookborrow_view")

    //根据学号和借阅时间进行分组，计算每次借阅的小记once_view
    val onceView = spark.sql("select XH,count(*) count,to_date(JSSJ) JSSJ from bookborrow_view group by XH, to_date(JSSJ)")
    onceView.createOrReplaceTempView("once_view")

    //根据once_view小记视图按照学号进行分组
    val result_view = spark.sql("select XH as outid,count(*) as frequency,sum(count) as total from once_view group by XH")
    //保存预处理数据
    result_view.write.saveAsTable("bookborrow_means_result")
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    //将数据追加到数据库
    result_view.select("outid","total","frequency").write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/schooldata", "bookborrow_total_result", prop)

    //测试
    spark.sql("select * from bookborrow_means_result where outid='201508040134'").show()
    spark.stop()

  }
  //方案一、这里是根据每个人每次借阅的书籍作为一条关联结果
  def getAprioriDataProcess(): Unit = {

    val sparkUtils = new SparkUtils
    val spark = sparkUtils.init()
    //创建需要处理的数据的临时视图bookborrow_view
    val sourceView = spark.sql("select * from bookborrow where TSMC != '' and FLH1 != ''")
    sourceView.createOrReplaceTempView("bookborrow_view")
    //根据学号和阶数时间进行分组，计算每次借阅的小记once_view
    val once_items_view = spark.sql("select XH as outid,concat_ws('\t', collect_set(TSMC)) book_items,concat_ws('\t', collect_set(FLH1)) cat_items,to_date(JSSJ) JSSJ from bookborrow_view group by XH, to_date(JSSJ)")
    once_items_view.createOrReplaceTempView("once_items_view")

    //保存预处理数据
    once_items_view.write.saveAsTable("once_items_view")
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    //将数据追加到数据库
    once_items_view.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/schooldata", "once_items_view", prop)

    //测试
    val result = spark.sql("select * from once_items_view where outid='201719144313'")
    result.collect.map(x => {
      println(x)
    })
    spark.stop()
  }
  //方案二、这里是根据每个人很多次即所有借阅作为一条关联记录
  def getAprioriDataProcess2(): Unit = {

    val sparkUtils = new SparkUtils
    val spark = sparkUtils.init()
    //创建需要处理的数据的临时视图bookborrow_view
    val sourceView = spark.sql("select * from bookborrow where TSMC != '' and FLH1 != ''")
    sourceView.createOrReplaceTempView("bookborrow_view")
    //根据学号和阶数时间进行分组，计算每次借阅的小记once_view
    val once_items_view = spark.sql("select XH as outid,concat_ws('\t', collect_set(TSMC)) book_items,concat_ws('\t', collect_set(FLH1)) cat_items from bookborrow_view group by XH")
    once_items_view.createOrReplaceTempView("student_items_view")

    //保存预处理数据
    once_items_view.write.saveAsTable("student_items_view")
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    //将数据追加到数据库
    once_items_view.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/schooldata", "student_items_view", prop)

    //测试
    val result = spark.sql("select * from student_items_view where outid='201719144313'")
    result.collect.map(x => {
      println(x)
    })
    spark.stop()
  }

}

object BookDataProcessing {
  def main(args: Array[String]): Unit = {
    val bookDataProcessing = new BookDataProcessing
    //bookDataProcessing.getKmeanDataProcess()
    bookDataProcessing.getAprioriDataProcess2()
  }
}
