package com.itwang.Utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * 用流的方式来操作hdfs上的文件
 * 可以实现读取指定偏移量范围的数据
 * @author 王亚强
 *
 */
public class HdfsStreamAccess {
	FileSystem fs = null;
	Configuration conf = null;
	@Before
	public void init() throws Exception{
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://slave1:9000");
		
		//拿到一个文件系统操作的客户端实例对象
		fs = FileSystem.get(conf);
		//可以直接传入 uri和用户身份
		fs = FileSystem.get(new URI("hdfs://slave1:9000"),conf,"hadoop"); //最后一个参数为用户名
	}
	/**
	 * 通过流方式上传文件
	 * @throws Exception
	 */
	@Test
	public void testUpload() throws Exception{
		FSDataOutputStream outputStream = fs.create(new Path("/angelbabay"), true);
		FileInputStream inputStream = new FileInputStream("c:/angelbaby");
		IOUtils.copy(inputStream, outputStream);
	}
	
	/**
	 * 通过流方式下载文件
	 * @throws Exception
	 */
	@Test
	public void testDownload() throws Exception{
		FSDataInputStream inputStream = fs.open(new Path("/angelbabay"));
		FileOutputStream outputStream = new FileOutputStream("d://angelbabay");
		IOUtils.copy(inputStream, outputStream);
	}
	/**
	 * 随机文件读取
	 * @throws Exception
	 */
	@Test
	public void testRandomAccess()throws Exception{
		FSDataInputStream inputStream = fs.open(new Path("/angelbabay"));
		//从指定位置读取
		inputStream.seek(15);
		FileOutputStream outputStream = new FileOutputStream("d://angelbabay.part");
		IOUtils.copy(inputStream, outputStream);
	}
	@Test
	public void testCat()throws Exception{
		FSDataInputStream inputStream = fs.open(new Path("/iloveyou"));
		IOUtils.copy(inputStream, System.out);
	}
}
