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
 * 该类计算不活跃客户的比例
 * @author uohzoaix
 *
 */
public class CBAActiveRatioByAppJob {

	//输入数据为CBAZombiesJob的结果
	//1003/30982 newData 0.0323736363 
	//3324/173362 newData3 0.01917375203   1.415824864523
	public static class CBAMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final Text outkey = new Text();
		private final IntWritable outvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String log = value.toString();
			if(!"".equals(log.split("\t")[1]) && !"NULL".equalsIgnoreCase(log.split("\t")[1]) && log.split("\t")[1] != null){
				outkey.set(log.split("\t")[0]);
				outvalue.set(1);
				context.write(outkey, outvalue);
			}
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
			System.err.println("Usage: CBAActiveRatioByAppJob <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "CBAActiveRatioByAppJob");
		job.setJarByClass(CBAActiveRatioByAppJob.class);
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
