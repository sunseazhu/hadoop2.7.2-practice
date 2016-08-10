package cn.edu.nupt.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	// 框架在map处理完成之后，将所有的kv对缓存起来，进行分组，然后传递一个组
	// <key,{value1,value2...valuen}>
	//<hello,{1,1,1,1,1,1.....}>
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {

		long count = 0;
		for(LongWritable value:values){
			
			count += value.get();
		}
		context.write(key, new LongWritable(count));
	}
}
