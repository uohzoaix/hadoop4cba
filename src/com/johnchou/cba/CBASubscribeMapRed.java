package com.johnchou.cba;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 该类计算父类下的子类的点击数，这些子类的点击数即为点击父类的数量，如：
 * corpID	productID	parentID	clickNum	clickDate
 * 11			2			NULL		3		2013-11-20
 * 11			23			NULL		1		2013-11-20
 * 11			NULL		1			2		2013-11-20
 * 11			2			1			NULL	2013-11-20
 * 该类计算的就是第四行的clickNum值，该值等于第三行的clickNum，因为第四行的parentID等于第三行的parentID，并且productID不为NULL
 * productID为NULL表示点击的是父类
 * @author uohzoaix
 *
 */
public class CBASubscribeMapRed {

	public static class CBAMap extends Mapper<LongWritable, Text, Text, Text> {
		private final Text outkey = new Text();
		private final Text outvalue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String log = value.toString();
			if (log.split("\t").length > 4) {
				outkey.set(log.split("\t")[0]);
				outvalue.set(log.split("\t")[1] + "\t" + log.split("\t")[2] + "\t" + log.split("\t")[3]);
				context.write(outkey, outvalue);
			}
		}
	}

	/**
	 * select subscribe from usersubscribe a inner join clicks b on
	 * a.subscribe=b.subscribe where
	 * 
	 * @author user
	 * 
	 */
	public static class CBAReduce extends Reducer<Text, Text, Text, IntWritable> {
		private IntWritable resultValue = new IntWritable();
		private Text resultKey = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Integer> parent2Num = new HashMap<String, Integer>();
			Map<String, String> waitFordo = new HashMap<String, String>();
			for (Text val : values) {
				String[] vals = val.toString().split("\t");
				if (vals[0] == null || "".equals(vals[0]) || "NULL".equalsIgnoreCase(vals[0])) {
					String parentID = vals[1];
					Integer clickNum = Integer.valueOf(vals[2]);
					parent2Num.put(parentID, clickNum);
				} else if (vals[2] != null && !"".equals(vals[2]) && !"NULL".equalsIgnoreCase(vals[2])) {
					resultKey.set(key + "\t" + vals[0]);
					resultValue.set(Integer.valueOf(vals[2]) * 5);
					context.write(resultKey, resultValue);
				} else {
					if (!parent2Num.containsKey(vals[1])) {
						waitFordo.put(vals[0], vals[1]);
					} else {
						resultKey.set(key + "\t" + vals[0]);
						resultValue.set(parent2Num.get(vals[1]));
						context.write(resultKey, resultValue);
					}
				}
			}
			if (waitFordo.size() > 0) {
				for (Map.Entry<String, String> entry : waitFordo.entrySet()) {
					resultKey.set(key + "\t" + entry.getKey());
					resultValue.set(parent2Num.get(entry.getValue()));
					context.write(resultKey, resultValue);
				}
			}
		}
	}

	//	public static class CBAReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	//
	//		@Override
	//		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	//			for (IntWritable val : values) {
	//				context.write(key, val);
	//			}
	//		}
	//	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "CBASubscribeMapRed");
		job.setJarByClass(CBASubscribeMapRed.class);
		job.setMapperClass(CBAMap.class);
		//job.setCombinerClass(CBACombine.class);
		job.setReducerClass(CBAReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
