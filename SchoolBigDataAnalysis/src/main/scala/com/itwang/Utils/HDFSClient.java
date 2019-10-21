package com.itwang.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * 
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * 
 * 也可以在构造客户端fs对象时，通过参数传递进去
 * @author 王亚强
 *
 */
public class HDFSClient {
	private String fs_defaultFS = null;
	private String hadoop_user_name = null;
	public FileSystem init() throws Exception{
		InputStream inputStream = HDFSClient.class.getClassLoader().getResourceAsStream("resource/HDFS.properties");
		Properties properties = new Properties();
		properties.load(inputStream);
		fs_defaultFS = properties.getProperty("fs.defaultFS");
		hadoop_user_name = properties.getProperty("fs.HADOOP_USER_NAME");
		Configuration conf =new Configuration();
		conf.set("fs.defaultFS", fs_defaultFS);
		//  如果在windows本地跑，需要从widnows访问HDFS，需要指定一个合法的身份
		//System.setProperty("HADOOP_USER_NAME", "hadoop");
		//拿到一个文件系统操作的客户端实例对象
		/*fs = FileSystem.get(conf);*/
		//可以直接传入 uri和用户身份
		FileSystem fs = FileSystem.get(new URI(fs_defaultFS),conf,hadoop_user_name); //最后一个参数为用户名
		return fs;
	}
	/**
	 * @Method
	 * @Author 王亚强
	 * @Version  1.0
	 * @Description 将本地文件上传到HDFS上去
	 * @param localPath remotePath
	 * @Return
	 * @Exception
	 * @Date 2019-03-29 10:47
	 */
	public void uploadFile(String localPath, String remotePath) throws Exception {
		FileSystem fs = init();
		fs.copyFromLocalFile(new Path(localPath), new Path(remotePath));
		fs.close();
	}
	
	public void downloadFile(String remotePath, String localPath) throws Exception {
		FileSystem fs = init();
		fs.copyToLocalFile(new Path(remotePath), new Path(localPath));
		fs.close();
	}
	
	public void getConf() throws Exception {
		FileSystem fs = init();
		Configuration conf = fs.getConf();
		Iterator<Entry<String, String>> iterator = conf.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			System.out.println(entry.getValue() + "--" + entry.getValue());//conf加载的内容
		}
		fs.close();
	}
	
	/**
	 * 创建目录
	 */
	public void makeHDFSDir(String dirPath) throws Exception {
		FileSystem fs = init();
		boolean mkdirs = fs.mkdirs(new Path(dirPath));
		System.out.println(mkdirs);
		fs.close();
	}
	
	/**
	 * 删除
	 */
	public void delete(String remotePath) throws Exception{
		FileSystem fs = init();
		boolean delete = fs.delete(new Path(remotePath), true);//true， 递归删除
		System.out.println(delete);
		fs.close();
	}
	
	public void listAllFile(String pathString) throws Exception{
		FileSystem fs = init();
		FileStatus[] listStatus = fs.listStatus(new Path(pathString));
		for (FileStatus fileStatus : listStatus) {
			System.err.println(fileStatus.getPath()+"================="+fileStatus.toString());
		}
		//会递归找到所有的文件
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(pathString), true);
		while(listFiles.hasNext()){
			LocatedFileStatus next = listFiles.next();
			String name = next.getPath().getName();
			Path path = next.getPath();
			System.out.println(name + "---" + path.toString());
		}
		fs.close();
	}
	/**
	 * 查看目录信息，只显示文件
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	public void listFiles(String pathString) throws Exception {
		FileSystem fs = init();
		// 思考：为什么返回迭代器，而不是List之类的容器
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(pathString), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println(fileStatus.getPath().getName());
			System.out.println(fileStatus.getBlockSize());
			System.out.println(fileStatus.getPermission());
			System.out.println(fileStatus.getLen());
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation bl : blockLocations) {
				System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
				String[] hosts = bl.getHosts();
				for (String host : hosts) {
					System.out.println(host);
				}
			}
			System.out.println("----------------------------");
		}
		fs.close();
	}

	/**
	 * 查看文件及文件夹信息
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	public void listAll(String pathString) throws Exception {
		FileSystem fs = init();
		FileStatus[] listStatus = fs.listStatus(new Path(pathString));

		String flag = "d--             ";
		for (FileStatus fstatus : listStatus) {
			if (fstatus.isFile())  flag = "f--         ";
			System.out.println(flag + fstatus.getPath().getName());
		}
		fs.close();
	}

	public void catHDFSFile(String remotePathString) throws Exception{
		FileSystem fs = init();
		FSDataInputStream in = fs.open(new Path(remotePathString));
		//拿到文件信息
		FileStatus[] listStatus = fs.listStatus(new Path(remotePathString));
		//获取这个文件的所有block的信息
		BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(listStatus[0], 0L, listStatus[0].getLen());
		//第一个block的长度
		long length = fileBlockLocations[0].getLength();
		//第一个block的起始偏移量
		long offset = fileBlockLocations[0].getOffset();
		
		System.out.println(length);
		System.out.println(offset);
		
		//获取第一个block写入输出流
//		IOUtils.copyBytes(in, System.out, (int)length);
		byte[] b = new byte[4096];
		
		FileOutputStream os = new FileOutputStream(new File("E:/block0"));
		while(in.read(offset, b, 0, 4096)!=-1){
			os.write(b);
			offset += 4096;
			if(offset>=length) return;
		};
		os.flush();
		os.close();
		in.close();
		fs.close();
	}
	public static void main(String[] args) throws Exception {
		HDFSClient hdfsClient = new HDFSClient();
//		hdfsClient.uploadFile("E:\\a.txt","/data/");
		//hdfsClient.delete("/data/a.txt");
		hdfsClient.downloadFile("/data/world1.txt", "G://Download/");
//		hdfsClient.catHDFSFile("/data/tree1.data");
	}
	

}
