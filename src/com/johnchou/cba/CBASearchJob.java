package com.johnchou.cba;

import java.io.IOException;
import java.util.regex.Matcher;

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
 * 该job分析/mobile/queryQuanPublishBySearch方法，可以知道客户对哪些圈子比较感兴趣，用于圈子推荐。
 * @author user
 *
 */
public class CBASearchJob {

	public static class CBAMap extends Mapper<LongWritable, Text, TextPair, IntWritable> {
		private final TextPair outkey = new TextPair(new Text(), new Text());
		private final IntWritable outvalue = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() != 0) {// 过滤掉第一行，第一行为注释
				String log = value.toString();
				///newData3	1688407	/mobile/queryQuanPublishBySearch	[1688407,null,"message",null,null,2,20]	2013-11-14 08:43:03
				Matcher matcher = MapRedUtil.patternWithCorpIDAndArg.matcher(log);
				if (matcher.matches() && matcher.groupCount() == 5) {
					if (matcher.group(3).equals("/mobile/queryQuanPublishBySearch")) {
						String groupID = matcher.group(4).split(",")[1];
						if(!"null".equalsIgnoreCase(groupID)&&!"".equals(groupID)&&groupID!=null){
							outkey.setFirstText(matcher.group(2));
							outkey.setSecondText(groupID);
							context.write(outkey, outvalue);
						}
					}
				}
			}
		}
	}

	public static class CBAReduce extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
			System.err.println("Usage: CBASearchJob <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "CBASearchJob");
		job.setJarByClass(CBASearchJob.class);
		job.setMapperClass(CBAMap.class);
		//job.setCombinerClass(CBACombine.class);
		job.setReducerClass(CBAReduce.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
