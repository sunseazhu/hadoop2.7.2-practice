package cn.edu.nupt.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 *<p> WCRunner.java
 *	Description:<br/> 
 *	(1)用来描述一个作业<br/>
 *	(2)比如，该作业使用哪个类作为逻辑处理中的map，哪个作为reduce
 *	(3)还可以指定改作业要处理的数据所在的路径
 *	(4)还可以指定作业输出的路径
 *<p>
 *	Company: cstor
 *
 *	@author zhuxy
 *	2016年8月4日 下午9:58:02
 */
public class WCRunner {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job wcjob = Job.getInstance(conf);
		
		// 找到Mapper和Reducer两个类所在的路径
		//设置整个job所用的那些类在哪个jar下
		wcjob.setJarByClass(WCRunner.class);
		
		//本job使用的mapper和reducer类
		wcjob.setMapperClass(WCMapper.class);
		wcjob.setReducerClass(WCReducer.class);
		
		//指定reduce的输出数据kv类型
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(LongWritable.class);
		
		// 指定map的输出数据的kv类型
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(LongWritable.class);
		
//		
		FileInputFormat.setInputPaths(wcjob, new Path("hdfs://master:9000/wc/input/testHdfs.txt"));
		FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://master:9000/wc/output9/"));

//		FileInputFormat.setInputPaths(wcjob, new Path("file:///E:/input/testwc.txt"));
//		FileOutputFormat.setOutputPath(wcjob, new Path("file:///E:/output3/"));
		
		wcjob.waitForCompletion(true);
	}
}
