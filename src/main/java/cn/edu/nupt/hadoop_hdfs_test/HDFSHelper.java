package cn.edu.nupt.hadoop_hdfs_test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.InitializationError;

/**
 * HDFS测试程序
 *<p>
 *	Description: 第一次安装好Eclipse+Hadoop+Maven
 *<p>
 *	Company: nupt
 *
 *	@author zhuxy
 *	2016年8月2日 下午9:48:32
 */
public class HDFSHelper {

	// 定义一个文件系统对象
	FileSystem fs = null;

	// before先于test执行
	@Before
	public void init() throws IOException {

		Configuration conf = new Configuration();

		// 在代码中配置configuration中的信息,
		//此处也可将hadoop配置的/etc/core-sit.xml直接拷贝到当前项目源码文件夹下
		//conf.set("fs.defaultFS", "hdfs://master:9000/");

		fs = FileSystem.get(conf);
	}

	//下载,比较底层的写法
	@Test
	public void downloadFromHDFS() throws IOException {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path("hdfs://master:9000/jdk-8u73-linux-x64.tar.gz");

		FSDataInputStream inputStream = fs.open(srcPath);
		FileOutputStream output = new FileOutputStream("E:/jdk-8u73-linux-x64.tar.gz");
		org.apache.commons.io.IOUtils.copy(inputStream, output);

		System.out.println("传输完成");
	}

	// 下载2，使用封装好的方法
	@Test
	public void downloadFromHdfs2() throws IllegalArgumentException, IOException {
		fs.copyToLocalFile(new Path("hdfs://master:9000/jdk-8u73-linux-x64.tar.gz"), new Path("e:/"));
	}

	//列出当前目录下的所有文件
	@Test
	public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		// listFiles 给定hdfs文件路径递归输出目录下面的所有文件
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("hdfs://master:9000/"), true);

		while (files.hasNext()) {
			LocatedFileStatus fileStatus = files.next();

			Path filePath = fileStatus.getPath();
			//String fileName=filePath.getName();

			System.out.println(filePath.toString());
		}

		System.out.println("---------------下面是不带递归的---------------------");
		//listStatus 可以列出文件和文件夹的信息，但是不提供自带的递归遍历
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus status : listStatus) {

			String name = status.getPath().getName();
			System.out.println(name + (status.isDirectory() ? " is dir" : " is file"));
		}
	}

	// 上传
	@Test
	public void uploadFile() throws IllegalArgumentException, IOException {

		// 定义输入流
		FileInputStream is = new FileInputStream("e:/abc.txt");
		//定义输出流
		FSDataOutputStream os = fs.create(new Path("hdfs://master:9000/abc.txt"));

		IOUtils.copy(is, os);
		System.out.println("长传成功");
	}

	@Test
	public void  upload2() throws IllegalArgumentException, IOException{
		fs.copyFromLocalFile(new Path("e:/abc.txt"), new Path("hdfs://master:9000/"));

	}
	// 删除
	@Test
	public void deleteFile() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/abc.txt"), true);
	}

	// 创建文件夹
	@Test
	public void makeDirectory() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/aaa/bbb/ccc"));
	}

	
	public static void main(String[] args) throws IOException {
		System.out.println("Hello World!");
		new HDFSHelper().downloadFromHDFS();
	}
}
