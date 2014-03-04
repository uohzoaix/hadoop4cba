package com.johnchou.cba;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * 该类计算用户对订阅和为订阅的品种的点击数,包括点击父类时的计算（这时子类品种那一列为null，如果是点击子类则父类品种那一列为null），
 * @author uohzoaix
 *
 */
public class CBAMapRed {

	public static class CBAMap extends Mapper<LongWritable, Text, TextPair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		TextPair outputKey = new TextPair(new Text(), new Text());
		TextPair outputValue = new TextPair(new Text(), new Text());

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() != 0) {// 过滤掉第一行，第一行为注释
				String log = value.toString();
				if (log.split("\t").length > 4) {
					// 有参数
					Matcher matcher = MapRedUtil.patternWithCorpIDAndArg.matcher(log);
					if (matcher.matches() && matcher.groupCount() == 5) {
						if (matcher.group(3).equals("/mobile/queryQuoteAndArticle")) {
							String args = matcher.group(4).replace("\"", "");
							for (String arg : args.split(",")) {
								outputKey.setFirstText(matcher.group(2));
								outputKey.setSecondText(arg + "\t" + "");
								context.write(outputKey, one);
							}
						} else if (matcher.group(3).equals("/mobile/querySubscribe2")) {
							//{"parentID":22,"time":"2013-11-10 19:57:22.8"}
							Integer start = matcher.group(4).indexOf(":");
							Integer end = matcher.group(4).indexOf(",", start);
							String parentID = matcher.group(4).substring(start + 1, end);
							outputKey.setFirstText(matcher.group(2));
							outputKey.setSecondText("" + "\t" + parentID);
							context.write(outputKey, one);
						}
					}
				}
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
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "cba");
		job.setJarByClass(CBAMapRed.class);
		job.setMapperClass(CBAMap.class);
		job.setCombinerClass(CBAReduce.class);
		job.setReducerClass(CBAReduce.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
