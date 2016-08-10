package cn.edu.nupt.hadoop.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] fields = value.toString().split("\t");
		
		//初始化对象
		String phone_NB=fields[1];
		int up_flow=Integer.parseInt(fields[7]);
		int down_flow=Integer.parseInt(fields[8]);
		int up_down_flow=up_flow+down_flow;
		
		FlowBean fb=new FlowBean(phone_NB, up_flow, down_flow, up_down_flow);
		
		context.write(new Text(phone_NB), fb);
	}
}
