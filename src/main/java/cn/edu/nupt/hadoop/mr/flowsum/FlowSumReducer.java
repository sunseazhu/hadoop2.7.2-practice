package cn.edu.nupt.hadoop.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		// 定义汇总变量
		int up_flow_total=0;
		int down_flow_total=0;
		int up_down_flow_total=0;
		
		
		//String phone_NB=key.toString();
		for(FlowBean fb:values){
			up_flow_total+=fb.getUp_flow();
			down_flow_total+=fb.getDown_flow();
			up_down_flow_total+=fb.getUp_down_flow();
		}
		
		context.write(key, new FlowBean(key.toString(), up_flow_total, down_flow_total, up_down_flow_total));
	}	
	
}
