package com.johnchou.cba;

import java.io.IOException;

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
 * 该类合并点击子类和点击父类的数量和，输入为CBASubscribeMapRed的输出，如：
 * corpID	productID	parentID	clickNum	clickDate
 * 11			2			NULL		3		2013-11-20
 * 11			23			NULL		1		2013-11-20
 * 11			NULL		1			2		2013-11-20
 * 11			2			1			2		2013-11-20
 * 经过CBASubscribeMapRed计算后得出的结果如上，这时候需要将第一行和第四行的clickNum加起来变成以下形式
 * corpID	productID	parentID	clickNum	clickDate
 * 11			2			NULL		5		2013-11-20
 * 11			23			NULL		1		2013-11-20
 * 11			NULL		1			2		2013-11-20
 * @author user
 */
public class CBACombineJob {

	public static class CBAMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final Text outkey = new Text();
		private final IntWritable outvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String log = value.toString();
			outkey.set(log.split("\t")[0] + "\t" + log.split("\t")[1]);
			outvalue.set(Integer.valueOf(log.split("\t")[2]));
			context.write(outkey, outvalue);
		}
	}

	public static class CBAReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CBACombineJob <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "CBACombineJob");
		job.setJarByClass(CBACombineJob.class);
		job.setMapperClass(CBAMap.class);
		//job.setCombinerClass(CBACombine.class);
		job.setReducerClass(CBAReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
