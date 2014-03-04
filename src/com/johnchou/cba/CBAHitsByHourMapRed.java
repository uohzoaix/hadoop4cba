package com.johnchou.cba;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
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
 * 该类计算每小时的点击数量
 * @author user
 *
 */
public class CBAHitsByHourMapRed {

	// hdfs://localhost:8020/user/user/input01/datas2013-11-07.txt hdfs://localhost:8020/user/user/output01
	// 该数组定义了需要计算哪些东西（calculate for），.*表示计算所有的，/mobile/queryQuoteAndArticle表示只计算path为/mobile/queryQuoteAndArticle的
	public static final String[] calculate4 = { ".*" };
	public static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static class CBAMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static Text hour = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() != 0) {// 过滤掉第一行，第一行为注释
				String log = value.toString();
				if (log.split("\t").length == 5) {
					Matcher matcher = MapRedUtil.allPattern.matcher(log);
					if (matcher.matches() && matcher.groupCount() == 5) {
						for (String regex : calculate4) {
							if (matcher.group(3).matches(regex)) {
								try {
									Date time = dateFormatter.parse(matcher.group(5));
									Calendar calendar = GregorianCalendar.getInstance();
									calendar.setTime(time);
									hour.set(String.valueOf(calendar.get(Calendar.HOUR_OF_DAY)));
									context.write(hour, one);
								} catch (ParseException e) {
									e.printStackTrace();
								}
							}
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
			System.err.println("Usage: CBAHitsByHour <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "CBAHitsByHour");
		job.setJarByClass(CBAMapRed.class);
		job.setMapperClass(CBAMap.class);
		job.setCombinerClass(CBAReduce.class);
		job.setReducerClass(CBAReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
