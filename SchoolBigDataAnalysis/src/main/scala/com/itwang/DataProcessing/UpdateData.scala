package com.itwang.DataProcessing

import com.itwang.Utils.HDFSClient

/**
  * @ProjectName: SchoolBigDataAnalysis 
  * @Package: com.itwang.DataProcessing
  * @ClassName: UpdateData
  * @Author: hadoop
  * @Description: 对原始数据库导出的数据上传到HDFS上
  * @Date: 2019-04-01 20:07
  * @Version: 1.0
  */
class UpdateData {
  def uploadFile(): Unit ={
    val hDFSClient = new HDFSClient()
//    hDFSClient.uploadFile("E:/data/bookborrow/2015-2016-01.txt", "/data/")
//    hDFSClient.uploadFile("E:/data/bookborrow/2015-2016-02.txt", "/data/")
//    hDFSClient.uploadFile("E:/data/bookborrow/2016-2017-01.txt", "/data/")
//    hDFSClient.uploadFile("E:/data/bookborrow/2016-2017-02.txt", "/data/")
//    hDFSClient.uploadFile("E:/data/bookborrow/2017-2018-01.txt", "/data/")
//    hDFSClient.uploadFile("E:/data/bookborrow/2017-2018-02.txt", "/data/")
//    hDFSClient.uploadFile("E:/data/bookborrow/2018-2019-01.txt", "/data/")
//    hDFSClient.uploadFile("E:/data/bookborrow/2018-2019-02.txt", "/data/")
    hDFSClient.uploadFile("D:/shareData/schooldata/consumedata/2015consume.txt","/jikewang")
    hDFSClient.uploadFile("D:/shareData/schooldata/consumedata/2015consume2.txt","/jikewang")
//    hDFSClient.uploadFile("E:/data/tushuguanmenjin.txt","/data/menjin")
  }
}
object UpdateData{
  def main(args: Array[String]): Unit = {
    val updateData = new UpdateData
    updateData.uploadFile()
  }
}
