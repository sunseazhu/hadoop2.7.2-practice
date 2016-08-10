package cn.edu.nupt.hadoop_hdfs_test;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;
/**
 * HDFS测试程序
 *<p>
 *	Description: 第一次安装好Eclipse+Hadoop+Maven
 *<p>
 *	Company: cstor
 *
 *	@author zhuxy
 *	2016年8月2日 下午9:48:32
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
        System.out.println( "Hello World!" );
        new App().downloadFromHDFS();
    }
    
 // To download file from HDFS to local filesystem
    @Test
 	public void downloadFromHDFS() throws IOException{
 		Configuration conf=new Configuration();
 		
 		FileSystem fs=FileSystem.get(conf);
// 		Path srcPath=new Path("hdfs://master:9000/jdk-8u73-linux-x64.tar.gz");
 		Path srcPath=new Path("hdfs://master:9000/testHdfs.txt");
 		
 		FSDataInputStream inputStream=fs.open(srcPath);
 		FileOutputStream output=new FileOutputStream("E:/jdk-8u73-linux-x64.tar.gz");
 		org.apache.commons.io.IOUtils.copy(inputStream, output);
 		
 		System.out.println("传输完成");
 	}
    
    public void listFiles() {
    	// listFiles 给定hdfs文件路径递归输出目录下面的所有文件
    		//RemoteIterator<LocatedFileStatus> files=
	}
    
}

