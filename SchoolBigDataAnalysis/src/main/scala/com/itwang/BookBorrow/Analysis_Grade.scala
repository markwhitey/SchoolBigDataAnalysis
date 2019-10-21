//package com.itwang.BookBorrow
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
//  * @Package: com.itwang.BookBorrow
//  * @ClassName: KMeansAnalysis
//  * @Author: hadoop
//  * @Description:
//  * @Date: 2019-03-30 16:09
//  * @Version: 1.0
//  */
//class KMeansAnalysis_Grade {
//  //通过聚类分析学生的阅读兴趣，活跃读者、一般读者、惰性读者
//  def getReadingInterest(grade: String, numcluster: Integer, maxIterations: Integer): Unit = {
//    val spark = new SparkUtils().init()
//    val sc = spark.sparkContext
//    val kmeanData = spark.sql(s"select * from bookborrow_means_result where XH like '$grade%'")
//
//    val kmeanRDD = kmeanData.rdd
//    val parsedata = kmeanRDD.map {
//      case Row(xh, frequency, total) =>
//        var frequencyDouble = frequency.toString.toDouble
//        var totalDouble = total.toString.toDouble
//        val features = Array[Double](frequencyDouble, totalDouble)
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
//    model.save(sc, s"src/main/resources/KMeansModel$grade")
//    val sameModel = KMeansModel.load(sc, s"src/main/resources/KMeansModel$grade")
//    //用模型对我们到数据进行预测
//    val resrdd = kmeanRDD.map {
//      case Row(outid, frequency, total) =>
//        //提取到每一行到特征值
//        val features = Array[Double](frequency.toString.toDouble, total.toString.toDouble)
//        //将特征值转换成特征向量
//        val linevector = Vectors.dense(features)
//        //将向量输入model中进行预测，得到预测值
//        val prediction = sameModel.predict(linevector)
//        //封装结果数据
//        KmeanResult(outid.toString, frequency.toString.toInt, total.toString.toInt, prediction.toString.toInt)
//    }
//    import spark.implicits._
//    val dataFrame = resrdd.toDF()
//    //创建Properties存储数据库相关属性
//    val prop = new Properties()
//    prop.put("user", "root")
//    prop.put("password", "root")
//    //将数据追加到数据库
//    dataFrame.write.mode("append").jdbc("jdbc:mysql://localhost:3306/schooldata", "bookborrow_kmeans", prop)
//    spark.stop()
//  }
//}
//
//object KMeansAnalysis_Grade {
//  def main(args: Array[String]): Unit = {
//    val kMeansAnalysis = new KMeansAnalysis_Grade
//    val gradeArray = Array("2013", "2014", "2015", "2016", "2017","2018")
//    //    val gradeArray = Array("2015")
//    for (i <- gradeArray) {
//      println(s"===================$i analysis starting.....===================")
//      kMeansAnalysis.getReadingInterest(i, 4, 200)
//      println(s"===================$i analysis end===================")
//    }
//  }
//}
//
//case class KmeanResult(outid: String, frequency: Integer, total: Integer, interest: Integer)