package cn.edu.nupt.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// 4个泛型中，前两个是指定的mapper输入数据的类型
//map 和 reduce 的数据输入输出是以key-value的形式封装的
//默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中一行的其实偏移量，这一行的内容作为value
// JDK 中long string等使用jdk自带的序列化机制，序列化之后会携带很多附加信息，造成网络传输冗余，
//		所以Hadoop自己封装了一些序列化机制
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	// mapreduce框架每读一行就调用一次该方法
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//具体的业务写在这个方法中，而且我们业务要处理的数据已经被该框架传递进来
		// key是这一行的其实偏移量，value是文本内容
		String line = value.toString();
		
		String[] words = StringUtils.split(line, " ");
		
		for(String word : words){
			
			context.write(new Text(word), new LongWritable(1));
			
		}
		

	}

	
	
}
