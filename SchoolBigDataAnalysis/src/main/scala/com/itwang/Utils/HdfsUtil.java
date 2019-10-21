package com.itwang.Utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HdfsUtil {

	FileSystem fs=null;
	Configuration conf = null;
	@Before
	public void init() throws Exception
	{
		//读取classpath下的xxx-site.xml配置文件，并解析其内容，封装到conf对象中去
		//System.out.println("hello world");	
		conf=new Configuration();
		//也可以在代码中对conf中的配置信息进行手动设置，会覆盖配置文件中读取的值
		conf.set("fs.defaultFS", "hdfs://slave1:9000/");
		//根据配置信息去获取一个具体文件系统的客户端操作实例对象
		fs=FileSystem.get(new URI("hdfs://slave1:9000/"),conf,"hadoop");
		
	}
	/**
	 * 上传文件
	 * @throws IOException
	 */
	@Test
	public void upload() throws IOException
	{
		Path path=new Path("hdfs://slave1:9000/qingshu2.txt");
		FSDataOutputStream output=fs.create(path);
		FileInputStream input=new FileInputStream("G:/Download/qingshu2.txt");
		IOUtils.copy(input, output);
	}
	
	@Test
	public void upload2() throws Exception
	{
		fs.copyFromLocalFile(new Path("G://shangchuan/qingshu.txt"), new Path("hdfs://slave1:9000/qingshu2.txt"));
		fs.close();
	}
	/**
	 * 下载文件
	 */
	@Test
	public void download()throws Exception
	{
		fs.copyToLocalFile(false,new Path("hdfs://wangyaqiang1:9000/qingshu2.txt"), new Path("G://Download/qingshu2.txt"), true);
	}
	public void download2() throws Exception, IOException
	{
		FSDataInputStream  input=fs.open(new Path("/qingshu2.txt"));
		FileOutputStream output=new FileOutputStream("G://Download/qingshu.txt");
		IOUtils.copy(input, output);
	}
	/**
	 * 查看文件信息
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException
	{
		//listFiles列出的是文件的信息，而且提供递归遍历
		RemoteIterator<LocatedFileStatus> files=fs.listFiles(new Path("/"),true);
		while(files.hasNext())
		{
			LocatedFileStatus file=files.next();
			Path filePath=file.getPath();
			String fileName=filePath.getName();
			System.out.println(fileName);
		}
		System.out.println("--------------------");
		//listStatus可以列出文件和文件夹信息，但不提供递归遍历
		FileStatus[] listStatus=fs.listStatus(new Path("/"));
		for(FileStatus status:listStatus)
		{
			String name=status.getPath().getName();
			System.out.println(name+(status.isDirectory()?" is dir":" is file"));
		}
	}
	/**
	 * 递归创建文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * 
	 */
	public void mkdir() throws IllegalArgumentException, IOException
	{
		fs.mkdirs(new Path("/aaa/bb"));
	}
	/**
	 * 
	 * 删除文件或文件夹
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	public void rm() throws IllegalArgumentException, IOException
	{
		fs.delete(new Path("/aaa"), true);
	}
	public void main(String[] args) throws Exception
	{
		init();
		//mkdir();
		//rm();
		//upload();
		//upload2();
		download2();
		//listFiles();
	}

}
