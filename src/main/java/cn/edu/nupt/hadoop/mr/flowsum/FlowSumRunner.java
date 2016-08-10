package cn.edu.nupt.hadoop.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlowSumRunner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(FlowSumRunner.class);
		
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)?1:0;
	}

	public static void main(String[] args) throws Exception {
		//hdfs://master:9000/wc/srcdata hdfs://master:9000/wc/outdata
		args=new String[2];
		args[0]="hdfs://master:9000/wc/srcdata";
		args[1]="hdfs://master:9000/wc/outdata";
		
		// 如何系统中存在输出文件的路径，将其删除
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000/");
		FileSystem fs=FileSystem.get(conf);
		
		if(fs.exists(new Path(args[1]))){
			fs.deleteOnExit(new Path(args[1]));
		}
		
		// 运行程序
		int res = ToolRunner.run(new Configuration(), new FlowSumRunner(), args);
		System.exit(res);
	}
	
	
}
