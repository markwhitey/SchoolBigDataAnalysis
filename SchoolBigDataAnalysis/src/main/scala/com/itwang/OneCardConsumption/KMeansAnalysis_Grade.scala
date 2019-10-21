//package com.itwang.OneCardConsumption
//
//import java.util.Properties
//
//import com.itwang.Utils.SparkUtils
//import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.sql.Row
//
///**
//  * @ProjectName: SchoolBigDataAnalysis
//  * @Package: com.itwang.OneCardConsumption
//  * @ClassName: DataAnalysis
//  * @Author: 王亚强
//  * @Description:
//  * @Date: 2019-03-27 17:32
//  * @Version: 1.0
//  */
//class KMeansAnalysis_Grade {
//  //通过聚类分析学生的阅读兴趣，活跃读者、一般读者、惰性读者
//  def getReadingInterest(grade: String, numcluster: Integer, maxIterations: Integer): Unit = {
//    val spark = new SparkUtils().init()
//    val sc = spark.sparkContext
//    val kmeanData = spark.sql(s"select * from consume_total_result where outid like '$grade%'")
//
//    val kmeanRDD = kmeanData.rdd
//    val parsedata = kmeanRDD.map {
//      case Row(outid, total, frequency) =>
//        var totalDouble = total.toString.toDouble
//        var frequencyDouble = frequency.toString.toDouble
//        val features = Array[Double](totalDouble, frequencyDouble)
//        //  将数组变成机器学习中的向量
//        Vectors.dense(features)
//    }.cache()
//
//    //用kmeans对样本向量进行训练得到模型
//    //指定最大迭代次数
//    val model = KMeans.train(parsedata, numcluster, maxIterations)
//    println("Cluster centers:")
//    model.clusterCenters.map(println(_))
//
//    // Evaluate clustering by computing Within Set Sum of Squared Errors
//    val WSSSE = model.computeCost(parsedata)
//    println("Within Set Sum of Squared Errors = " + WSSSE)
//    // Save and load model
//    model.save(sc, s"src/main/resources/consume_KMeansModel$grade")
//    val sameModel = KMeansModel.load(sc, s"src/main/resources/consume_KMeansModel$grade")
//    //用模型对我们到数据进行预测
//    val resrdd = kmeanRDD.map {
//      case Row(outid, total, frequency) =>
//        //提取到每一行到特征值
//        val features = Array[Double](total.toString.toDouble, frequency.toString.toDouble)
//        //将特征值转换成特征向量
//        val linevector = Vectors.dense(features)
//        //将向量输入model中进行预测，得到预测值
//        val prediction = sameModel.predict(linevector)
//        //封装结果数据
//        KmeanResult_Grade(outid.toString, total.toString.toInt, frequency.toString.toInt, prediction.toString.toInt)
//    }
//    import spark.implicits._
//    val dataFrame = resrdd.toDF()
//    //创建Properties存储数据库相关属性
//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "root")
//    //将数据追加到数据库
//    dataFrame.write.mode("append").jdbc("jdbc:mysql://localhost:3306/schooldata", "consume_total_kmeans", prop)
//    spark.stop()
//  }
//}
//
//object KMeansAnalysis_Grade {
//  def main(args: Array[String]): Unit = {
//    val kMeansAnalysis = new KMeansAnalysis_Grade
//    //    val gradeArray = Array("2013", "2014", "2015", "2016", "2017", "2018")
//    val gradeArray = Array("2015")
//    for (i <- gradeArray) {
//      println(s"===================$i analysis starting.....===================")
//      kMeansAnalysis.getReadingInterest(i, 5, 400)
//      println(s"===================$i analysis end===================")
//    }
//  }
//
//}
//
//case class KmeanResult_Grade(outid: String, total_amount: Integer, total_frequency : Integer, cluster: Integer)
