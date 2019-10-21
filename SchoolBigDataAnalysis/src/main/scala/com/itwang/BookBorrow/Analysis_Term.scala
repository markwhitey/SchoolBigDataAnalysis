package com.itwang.BookBorrow

import java.util.Properties

import com.itwang.DataProcessing.BookDataProcessing
import com.itwang.Utils.SparkUtils
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row

/**
  * @ProjectName: SchoolBigDataAnalysis 
  * @Package: com.itwang.BookBorrow
  * @ClassName: KMeansAnalysis
  * @Author: hadoop
  * @Description:
  * @Date: 2019-03-30 16:09
  * @Version: 1.0
  */
class Analysis_Term {
  //对学生的借阅总数和借阅频次和门禁频次聚类分析
  def getBookBorrowKmeans(numcluster: Integer, maxIterations: Integer): Unit = {
    val spark = new SparkUtils().init()
    val sc = spark.sparkContext
    //从hive仓库中获取所有分析数据
    val bookData = spark.sql("select outid,total,frequency from bookborrow_means_result")

    //查询出大学生进入图书馆的所有数据
    val mergeresult = spark.sql("select * from entranceguard_mergeresult")
    //创建临时视图
    mergeresult.createOrReplaceTempView("mergeresult_view")
    //根据上面的临时视图统计每个学生的进入图书馆的次数
    val entranceData = spark.sql("select outid, count(*) as frequency from mergeresult_view group by outid")
    //创建临时视图，为后面聚类分析提供依据
    entranceData.createOrReplaceTempView("entrance_view")
    //对学生所借的图书记录和总记录频次进行和门禁的频次进行比较
    val kmeanData = spark.sql("select b.outid, b.total, b.frequency, e.frequency from bookborrow_means_result b join entrance_view e on b.outid = e.outid")


    val maxData = spark.sql("select max(total),max(frequency) from bookborrow_means_result")

    var maxs = maxData.rdd.map(x => (x.get(0),x.get(1)))
    val tuples = maxs.take(1)
    val maxTotal = tuples(0)._1.toString.toDouble
    val maxFreq = tuples(0)._2.toString.toDouble

    val maxMenJinData = spark.sql("select max(frequency) from entrance_view")
    val tuple = maxMenJinData.take(1)
    val maxMenJin = tuple(0).get(0).toString.toDouble


    val kmeanRDD = kmeanData.rdd
    val parsedata = kmeanRDD.map {
      case Row(xh, total_book, frequency_book,frequency_menjin) =>
        var totalBookDouble = total_book.toString.toDouble
        var frequencyBookDouble = frequency_book.toString.toDouble
        var frequencyMenJinDouble = frequency_menjin.toString.toDouble
        //对每一级的学生计算平均借阅频率和总数
        if (xh.toString.matches("2013.{8}")) {
          totalBookDouble = (totalBookDouble / 4.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 4.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 4.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2014.{8}")) {
          totalBookDouble = (totalBookDouble / 6.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 6.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 6.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2015.{8}")) {
          totalBookDouble = (totalBookDouble / 7.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 7.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 7.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2016.{8}")) {
          totalBookDouble = (totalBookDouble / 5.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 5.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 5.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2017.{8}")) {
          totalBookDouble = (totalBookDouble / 3.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 3.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 3.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2018.{8}")) {
          totalBookDouble = (totalBookDouble / 1.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 1.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 1.0).formatted("%.2f").toDouble
        }
        val features = Array[Double](totalBookDouble / maxTotal, frequencyBookDouble / maxFreq, frequencyMenJinDouble / maxMenJin)
        //  将数组变成机器学习中的向量
        Vectors.dense(features)
    }.cache()

    //用kmeans对样本向量进行训练得到模型
    //指定最大迭代次数
    val model = KMeans.train(parsedata, numcluster, maxIterations)
    println("Cluster centers:")
    model.clusterCenters.map(println(_))

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = model.computeCost(parsedata)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    // Save and load model
    model.save(sc, s"src/main/resources/KMeansModel_bookborrow")
    val sameModel = KMeansModel.load(sc, s"src/main/resources/KMeansModel_bookborrow")
    //用模型对我们到数据进行预测
    val resrdd = kmeanRDD.map {
      case Row(xh, total_book, frequency_book,frequency_menjin) =>
        var totalBookDouble = total_book.toString.toDouble
        var frequencyBookDouble = frequency_book.toString.toDouble
        var frequencyMenJinDouble = frequency_menjin.toString.toDouble
        //对每一级的学生计算平均借阅频率和总数
        if (xh.toString.matches("2013.{8}")) {
          totalBookDouble = (totalBookDouble / 4.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 4.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 4.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2014.{8}")) {
          totalBookDouble = (totalBookDouble / 6.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 6.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 6.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2015.{8}")) {
          totalBookDouble = (totalBookDouble / 7.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 7.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 7.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2016.{8}")) {
          totalBookDouble = (totalBookDouble / 5.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 5.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 5.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2017.{8}")) {
          totalBookDouble = (totalBookDouble / 3.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 3.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 3.0).formatted("%.2f").toDouble
        }
        else if (xh.toString.matches("2018.{8}")) {
          totalBookDouble = (totalBookDouble / 1.0).formatted("%.2f").toDouble
          frequencyBookDouble = (frequencyBookDouble / 1.0).formatted("%.2f").toDouble
          frequencyMenJinDouble = (frequencyMenJinDouble / 1.0).formatted("%.2f").toDouble
        }
        val features = Array[Double](totalBookDouble / maxTotal, frequencyBookDouble / maxFreq, frequencyMenJinDouble / maxMenJin)

        //将特征值转换成特征向量
        val linevector = Vectors.dense(features)
        //将向量输入model中进行预测，得到预测值
        val prediction = sameModel.predict(linevector)
        //封装结果数据
        StudyKmeansResult(xh.toString, totalBookDouble, frequencyBookDouble, frequencyMenJinDouble, prediction)
    }
    import spark.implicits._
    val dataFrame = resrdd.toDF()
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    //将数据追加到数据库
    dataFrame.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/schooldata", "study_kmeans", prop)
    spark.stop()
  }
  //对每个学生的每次借阅的书籍进行关联分析
  def getBookItemsApriori(minSupport: Double, numPartition: Integer, minConfidence: Double): Unit = {
    val spark = new SparkUtils().init()
    val transactionsDF = spark.sql("select book_items from once_items_view")
    //获取读者每次借阅的所有书的集合
    val book_items_collection = transactionsDF.rdd.map(items => {
      var array: Array[String] = null
      items.get(0)
    })
    //对借书集合进行切分
    val transactions = book_items_collection.map(s => s.toString.split("\t")).cache()
    //    transactions.foreach(x => println(x.toBuffer))
    println(s"Number of transactions: ${transactions.count()}")
    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)

    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    /*    val resrdd = model.generateAssociationRules(minConfidence).foreachPartition(it => {
          var url = "jdbc:mysql://localhost:3306/schooldata?useUnicode=true&characterEncoding=utf8"
          val conn= DriverManager.getConnection(url,"root","root")
          val pstat = conn.prepareStatement ("INSERT INTO bookborrow_apriori (pre_items, latter_items,confidence) VALUES (?, ?, ?)")
          for (rule <- it){
            pstat.setString(1,rule.antecedent.mkString("\t"))
            pstat.setString(2,rule.consequent.mkString("\t"))
            pstat.setDouble(3,rule.confidence)
            pstat.addBatch
          }
          try{
            pstat.executeBatch
          }finally{
            pstat.close
            conn.close
          }
        })*/
    val resrdd = model.generateAssociationRules(minConfidence).map( rule =>
      BookItemsAprirorResult(rule.antecedent.mkString("\t"), rule.consequent.mkString("\t"), rule.confidence)
    )
    import spark.implicits._
    val dataFrame = resrdd.toDF()
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    //将数据追加到数据库
    dataFrame.write.mode("append").jdbc("jdbc:mysql://localhost:3306/schooldata", "bookborrow_apriori", prop)

    println(s"Number of frequent itemsets Confidence: ${model.generateAssociationRules(minConfidence).collect().length}")
    spark.stop()
  }
  //对每个学生的每次借阅的数据所属类别进行关联分析
  def getBookCatApriori(minSupport: Double, numPartition: Integer, minConfidence: Double): Unit = {
    val spark = new SparkUtils().init()
    val transactionsDF = spark.sql("select cat_items from once_items_view")
    //获取读者每次借阅的所有书的集合
    val book_items_collection = transactionsDF.rdd.map(items => {
      var array: Array[String] = null
      items.get(0)
    })
    //对借书集合进行切分
    val transactions = book_items_collection.map(s => s.toString.split("\t")).cache()
    //    transactions.foreach(x => println(x.toBuffer))
    println(s"Number of transactions: ${transactions.count()}")
    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)

    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    val resrdd = model.generateAssociationRules(minConfidence).map( rule =>
      BookItemsAprirorResult(rule.antecedent.mkString("\t"), rule.consequent.mkString("\t"), rule.confidence)
    )
    import spark.implicits._
    val dataFrame = resrdd.toDF()
    //创建Properties存储数据库相关属性
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    //将数据追加到数据库
    dataFrame.write.mode("append").jdbc("jdbc:mysql://localhost:3306/schooldata", "bookcat_apriori", prop)

    println(s"Number of frequent itemsets Confidence: ${model.generateAssociationRules(minConfidence).collect().length}")
    spark.stop()
  }

  //对每个学生的每次借阅的数据所属类别的前三个字母类别部分进行关联分析
  def getBookCatSubApriori(minSupport: Double, numPartition: Integer, minConfidence: Double): Unit = {
    val spark = new SparkUtils().init()
    val transactionsDF = spark.sql("select cat_items from once_items_view")
    //获取读者每次借阅的所有书的集合
    val book_items_collection = transactionsDF.rdd.map(items => {
      var array: Array[String] = null
      items.get(0)
    })
    //对借书集合进行切分
    val transactions = book_items_collection.map(s => s.toString.split("\t").map(x => {
      if(x.length < 3)
        x
      else
        x.substring(0,3)
    }).toSet.toArray).cache()
    //    transactions.foreach(x => println(x.toBuffer))
    println(s"Number of transactions: ${transactions.count()}")
    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)

    val totalCount = model.freqItemsets.count()
    println(s"Number of frequent itemsets: ${totalCount}")

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq + ", " + itemset.freq/totalCount)
    }
    val resrdd = model.generateAssociationRules(minConfidence).map( rule =>
      BookItemsAprirorResult(rule.antecedent.mkString("\t"), rule.consequent.mkString("\t"), rule.confidence)
    )
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
    println(s"Number of frequent itemsets Confidence: ${model.generateAssociationRules(minConfidence).collect().length}")
    spark.stop()
  }

  //方案一、对每个学生的每次借阅的数据所属类别的第一个字母进行关联分析
  def getBookCatSubApriori2(minSupport: Double, numPartition: Integer, minConfidence: Double): Unit = {
    val spark = new SparkUtils().init()
    val transactionsDF = spark.sql("select cat_items from once_items_view")
    //获取读者每次借阅的所有书的集合
    val book_items_collection = transactionsDF.rdd.map(items => {
      var array: Array[String] = null
      items.get(0)
    })
    //对借书集合进行切分
    val transactions = book_items_collection.map(s => s.toString.split("\t").map(x => {
      x.substring(0,1)
    }).toSet.toArray).cache()
    //    transactions.foreach(x => println(x.toBuffer))
    println(s"Number of transactions: ${transactions.count()}")
    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)

    val totalCount = model.freqItemsets.count()
    println(s"Number of frequent itemsets: ${totalCount}")

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq + ", " + itemset.freq / totalCount)
    }
    val resrdd = model.generateAssociationRules(minConfidence).map( rule =>
      BookItemsAprirorResult(rule.antecedent.mkString("\t"), rule.consequent.mkString("\t"), rule.confidence)
    )
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
    println(s"Number of frequent itemsets Confidence: ${model.generateAssociationRules(minConfidence).collect().length}")
    spark.stop()
  }
  //方案二、对每个学生的所有借阅的数据相当于一条记录的所属类别的第一个字母进行关联分析
  def getBookCatSubApriori3(minSupport: Double, numPartition: Integer, minConfidence: Double): Unit = {
    val spark = new SparkUtils().init()
    val transactionsDF = spark.sql("select cat_items from student_items_view")
    //获取读者每次借阅的所有书的集合
    val book_items_collection = transactionsDF.rdd.map(items => {
      var array: Array[String] = null
      items.get(0)
    })
    //对借书集合进行切分
    val transactions = book_items_collection.map(s => s.toString.split("\t").map(x => {
      x.substring(0,1)
    }).toSet.toArray).cache()
    //    transactions.foreach(x => println(x.toBuffer))
    println(s"Number of transactions: ${transactions.count()}")
    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)

    val totalCount = model.freqItemsets.count()
    println(s"Number of frequent itemsets: ${totalCount}")

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq + ", " + itemset.freq / totalCount)
    }
    val resrdd = model.generateAssociationRules(minConfidence).map( rule =>
      BookItemsAprirorResult(rule.antecedent.mkString("\t"), rule.consequent.mkString("\t"), rule.confidence)
    )
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
    println(s"Number of frequent itemsets Confidence: ${model.generateAssociationRules(minConfidence).collect().length}")
    spark.stop()
  }

  //对学生的生活规律、消费水平、学习强度进行关联分析
  def getBetweenStudentApriori(minSupport: Double, numPartition: Integer, minConfidence: Double): Unit = {
    val spark = new SparkUtils().init()
    //val transactionsDF = spark.sql("select t.cluster ,f.cluster,s.interest from consume_total_kmeans t join consume_freq_kmeans f on t.outid=f.outid join study_kmeans s on t.outid = s.outidselect t.cluster ,f.cluster,s.interest from consume_total_kmeans t join consume_freq_kmeans f on t.outid=f.outid join study_kmeans s on t.outid = s.outid")
    //获取读者每次借阅的所有书的集合
    val sc = spark.sparkContext
    val student_items_collection = sc.textFile("src/main/resources/apriori/Aprioridata.txt")
    val transactions = student_items_collection.map(s => {
      val splits = s.split("\t")
      val one = "2" + splits(0)
      val two = "3" + splits(1)
      val three = "4" + splits(2)
      Array[String](one,two, three)
    }).cache()
    //对借书集合进行切分
    println(s"Number of transactions: ${transactions.count()}")
    val model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions)

    val totalCount = model.freqItemsets.count()
    println(s"Number of frequent itemsets: ${totalCount}")

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq + ", " + (itemset.freq.toDouble / transactions.count()))
    }
    val resrdd = model.generateAssociationRules(minConfidence).map( rule =>
      BookItemsAprirorResult(rule.antecedent.mkString("\t"), rule.consequent.mkString("\t"), rule.confidence)
    )
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
    println(s"Number of frequent itemsets Confidence: ${model.generateAssociationRules(minConfidence).collect().length}")
    spark.stop()
  }


}

object Analysis_Term {
  def main(args: Array[String]): Unit = {
    //数据预处理
    //val dataProcessing = new BookDataProcessing
    //dataProcessing.loadData()
    //dataProcessing.getKmeanDataProcess()
    //dataProcessing.getAprioriDataProcess()
    val Analysis = new Analysis_Term

    //聚类分析
    //Analysis.getBookBorrowKmeans(4, 800)
    //关联分析
    //Analysis.getBookCatApriori(0.005,10,0.04)
    //aprioriAnalysis.getBookItemsApriori(0.00001, 8,0.0000008)
    //    aprioriAnalysis.getBookCatSubApriori(0.0005,10,0.004)
    //    aprioriAnalysis.getBookCatSubApriori2(0.0005,10,0.004)
    //Analysis.getBookCatSubApriori3(0.1,10,0.6)
    Analysis.getBetweenStudentApriori(0.001,10,0.6)
  }

}

case class StudyKmeansResult(outid: String,total_book: Double,frequency_book: Double, frequency_menjin: Double, interest: Integer)
case class BookItemsAprirorResult(pre_items: String, latter_items: String, confidence: Double)