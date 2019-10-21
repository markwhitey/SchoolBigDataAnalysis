package com.itwang.OneCardConsumption

import java.util.Properties

import com.itwang.DataProcessing.ConsumerProcessing
import com.itwang.Utils.SparkUtils
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row

/**
  * @ProjectName: SchoolBigDataAnalysis 
  * @Package: com.itwang.OneCardConsumption
  * @ClassName: DataAnalysis
  * @Author: 王亚强
  * @Description:
  * @Date: 2019-03-27 17:32
  * @Version: 1.0
  */
class KMeansAnalysis_Term {
  //通过聚类分析学生的一卡通在2013-9到2018-1的消费总额和消费总频次
  def getConsumeTotalKmeans(numcluster: Integer, maxIterations: Integer): Unit = {
    val spark = new SparkUtils().init()
    val sc = spark.sparkContext
    val kmeanData = spark.sql("select * from consume_total_result")

    val kmeanRDD = kmeanData.rdd
    val parsedata = kmeanRDD.map {
      case Row(outid, total, frequency) =>
        var frequencyDouble = frequency.toString.toDouble
        var totalDouble = total.toString.toDouble
        //对每一级的学生计算平均借阅频率和总数,这里是从13年开始的数据到18年1月的
        if (outid.toString.matches("2013.{8}")) {
          frequencyDouble = (frequencyDouble / 8.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 8.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2014.{8}")) {
          frequencyDouble = (frequencyDouble / 7.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 7.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2015.{8}")) {
          //          frequencyDouble = (frequencyDouble / 1.0).formatted("%.2f").toDouble
          //          totalDouble = (totalDouble / 1.0).formatted("%.2f").toDouble
          frequencyDouble = (frequencyDouble / 5.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 5.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2016.{8}")) {
          frequencyDouble = (frequencyDouble / 3.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 3.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2017.{8}")) {
          frequencyDouble = (frequencyDouble / 1.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 1.0).formatted("%.2f").toDouble
        }
        val features = Array[Double](totalDouble, frequencyDouble)
        //  将数组变成机器学习中的向量
        Vectors.dense(features)
    }.cache()

    //用kmeans对样本向量进行训练得到模型
    //指定最大迭代次数
    val model = KMeans.train(parsedata, 4, 800)
    println("Cluster centers:")
    model.clusterCenters.map(println(_))

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = model.computeCost(parsedata)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    // Save and load model
    //model.save(sc, s"src/main/resources/consume_KMeansModel_total")
    //val sameModel = KMeansModel.load(sc, s"src/main/resources/consume_KMeansModel_total")
    //用模型对我们到数据进行预测
    val resrdd = kmeanRDD.map {
      case Row(outid, total, frequency) =>
        var frequencyDouble = frequency.toString.toDouble
        var totalDouble = total.toString.toDouble
        //对每一级的学生计算平均借阅频率和总数,这里是从13年开始的数据到18年1月的
        if (outid.toString.matches("2013.{8}")) {
          frequencyDouble = (frequencyDouble / 8.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 8.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2014.{8}")) {
          frequencyDouble = (frequencyDouble / 7.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 7.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2015.{8}")) {
//          frequencyDouble = (frequencyDouble / 1.0).formatted("%.2f").toDouble
//          totalDouble = (totalDouble / 1.0).formatted("%.2f").toDouble
          frequencyDouble = (frequencyDouble / 5.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 5.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2016.{8}")) {
          frequencyDouble = (frequencyDouble / 3.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 3.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2017.{8}")) {
          frequencyDouble = (frequencyDouble / 1.0).formatted("%.2f").toDouble
          totalDouble = (totalDouble / 1.0).formatted("%.2f").toDouble
        }
        //提取到每一行到特征值
        val features = Array[Double](totalDouble, frequencyDouble)
        //将特征值转换成特征向量
        val linevector = Vectors.dense(features)
        //将向量输入model中进行预测，得到预测值
        val prediction = model.predict(linevector)
        //封装结果数据
        KmeanResult_Term(outid.toString, total.toString.toDouble, frequency.toString.toDouble, prediction.toString.toInt)
    }
    import spark.implicits._
    val dataFrame = resrdd.toDF()
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "hadoop")
    //将数据追加到数据库
    dataFrame.write.mode("append").jdbc("jdbc:mysql://39.105.128.239:3306/wang", "consume_total_kmeans", prop)
    spark.stop()
  }

  //通过聚类分析学生的一卡通在2013-9到2018-1的早餐频次，午餐频次，晚餐频次，商场购物频次，用水频次
  def getConsumeFrequencyKmeans(numcluster: Integer, maxIterations: Integer): Unit = {
    val spark = new SparkUtils().init()
    val sc = spark.sparkContext
    val breakfast = spark.sql("select outid,frequency from consume_student_result where dscrp = 'breakfast'")
    breakfast.createTempView("t_breakfast")
    val lunch = spark.sql("select outid,frequency from consume_student_result where dscrp = 'lunch'")
    lunch.createTempView("t_lunch")
    val dinner = spark.sql("select outid,frequency from consume_student_result where dscrp = 'dinner'")
    dinner.createTempView("t_dinner")
    val water = spark.sql("select outid,frequency from consume_student_result where dscrp = '用水支出'")
    water.createTempView("t_water")
    val shop = spark.sql("select outid,frequency from consume_student_result where dscrp = '商场购物'")
    shop.createTempView("t_shop")
    val kmeanData = spark.sql("select b.outid,b.frequency as breakfast,l.frequency as lunch,d.frequency as dinner,w.frequency as water,s.frequency as shop from t_breakfast b join t_lunch l on b.outid=l.outid join t_dinner d on b.outid = d.outid join t_water w on b.outid = w.outid join t_shop s on b.outid = s.outid")
    val kmeanRDD = kmeanData.rdd
    val parsedata = kmeanRDD.map {
      case Row(outid, breakfast, lunch, dinner, water, shop) =>
        var breakfastDouble = breakfast.toString.toDouble
        var lunchDouble = lunch.toString.toDouble
        var dinnerDouble = dinner.toString.toDouble
        var waterDouble = water.toString.toDouble
        var shopDouble = shop.toString.toDouble
        //对每一级的学生计算平均借阅频率和总数,这里是从13年开始的数据到18年1月的
        if (outid.toString.matches("2013.{8}")) {
          breakfastDouble = (breakfastDouble / 8.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 8.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 8.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 8.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 8.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2014.{8}")) {
          breakfastDouble = (breakfastDouble / 7.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 7.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 7.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 7.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 7.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2015.{8}")) {
//          breakfastDouble = (breakfastDouble / 1.0).formatted("%.2f").toDouble
//          lunchDouble = (lunchDouble / 1.0).formatted("%.2f").toDouble
//          dinnerDouble = (dinnerDouble / 1.0).formatted("%.2f").toDouble
//          waterDouble = (waterDouble / 1.0).formatted("%.2f").toDouble
//          shopDouble = (shopDouble / 1.0).formatted("%.2f").toDouble
          breakfastDouble = (breakfastDouble / 5.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 5.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 5.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 5.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 5.0).formatted("%.2f").toDouble

        }
        else if (outid.toString.matches("2016.{8}")) {
          breakfastDouble = (breakfastDouble / 3.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 3.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 3.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 3.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 3.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2017.{8}")) {
          breakfastDouble = (breakfastDouble / 1.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 1.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 1.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 1.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 1.0).formatted("%.2f").toDouble
        }
        val features = Array[Double](breakfastDouble, lunchDouble, dinnerDouble, waterDouble, shopDouble)
        //  将数组变成机器学习中的向量
        Vectors.dense(features)
    }.cache()

    //用kmeans对样本向量进行训练得到模型
    //指定最大迭代次数
    val model = KMeans.train(parsedata, 4, 800)
    println("Cluster centers:")
    model.clusterCenters.map(println(_))

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = model.computeCost(parsedata)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    // Save and load model
    //model.save(sc, s"src/main/resources/consume_KMeansModel_frequency")
    //val sameModel = KMeansModel.load(sc, s"src/main/resources/consume_KMeansModel_frequency")
    //用模型对我们到数据进行预测
    val resrdd = kmeanRDD.map {
      case Row(outid, breakfast, lunch, dinner, water, shop) =>
        var breakfastDouble = breakfast.toString.toDouble
        var lunchDouble = lunch.toString.toDouble
        var dinnerDouble = dinner.toString.toDouble
        var waterDouble = water.toString.toDouble
        var shopDouble = shop.toString.toDouble
        //对每一级的学生计算平均借阅频率和总数,这里是从13年开始的数据到18年1月的
        if (outid.toString.matches("2013.{8}")) {
          breakfastDouble = (breakfastDouble / 8.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 8.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 8.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 8.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 8.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2014.{8}")) {
          breakfastDouble = (breakfastDouble / 7.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 7.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 7.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 7.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 7.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2015.{8}")) {
          //          breakfastDouble = (breakfastDouble / 1.0).formatted("%.2f").toDouble
          //          lunchDouble = (lunchDouble / 1.0).formatted("%.2f").toDouble
          //          dinnerDouble = (dinnerDouble / 1.0).formatted("%.2f").toDouble
          //          waterDouble = (waterDouble / 1.0).formatted("%.2f").toDouble
          //          shopDouble = (shopDouble / 1.0).formatted("%.2f").toDouble
          breakfastDouble = (breakfastDouble / 5.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 5.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 5.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 5.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 5.0).formatted("%.2f").toDouble

        }
        else if (outid.toString.matches("2016.{8}")) {
          breakfastDouble = (breakfastDouble / 3.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 3.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 3.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 3.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 3.0).formatted("%.2f").toDouble
        }
        else if (outid.toString.matches("2017.{8}")) {
          breakfastDouble = (breakfastDouble / 1.0).formatted("%.2f").toDouble
          lunchDouble = (lunchDouble / 1.0).formatted("%.2f").toDouble
          dinnerDouble = (dinnerDouble / 1.0).formatted("%.2f").toDouble
          waterDouble = (waterDouble / 1.0).formatted("%.2f").toDouble
          shopDouble = (shopDouble / 1.0).formatted("%.2f").toDouble
        }
        val features = Array[Double](breakfastDouble, lunchDouble, dinnerDouble, waterDouble, shopDouble)
        //将特征值转换成特征向量
        val linevector = Vectors.dense(features)
        //将向量输入model中进行预测，得到预测值
        val prediction = model.predict(linevector)
        //封装结果数据
        KmeanFrequency_Term(outid.toString, breakfastDouble, lunchDouble, dinnerDouble, waterDouble, shopDouble, prediction)
    }
    import spark.implicits._
    val dataFrame = resrdd.toDF()
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "hadoop")
    prop.put("password", "hadoop")
    //将数据追加到数据库
    dataFrame.write.mode("append").jdbc("jdbc:mysql://39.105.128.239:3306/wang", "consume_freq_kmeans", prop)
    spark.stop()
  }
}

object KMeansAnalysis_Term {
  def main(args: Array[String]): Unit = {
    val kMeansAnalysis = new KMeansAnalysis_Term
    //数据预处理
    val consumeProcessing = new ConsumerProcessing
    consumeProcessing.loadData()
    consumeProcessing.getDetailConsume()
    consumeProcessing.getStatisticsData()
    //数据聚类分析
    //对一卡通总消费额和总消费频次聚类分析
    kMeansAnalysis.getConsumeTotalKmeans(4, 800)
    //对一卡通各种消费频次进行聚类分析
    kMeansAnalysis.getConsumeFrequencyKmeans(4, 800)
  }

}

case class KmeanResult_Term(outid: String, total_amount: Double, total_frequency: Double, cluster: Integer)

case class KmeanFrequency_Term(outid: String, breakfast_fre: Double, lunch_fre: Double, dinner_fre: Double, water_fre: Double, shopping_fre: Double, cluster: Integer)
