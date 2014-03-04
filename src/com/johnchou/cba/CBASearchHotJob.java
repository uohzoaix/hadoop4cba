package com.johnchou.cba;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import redis.clients.jedis.Jedis;

/**
 * 搜索热词榜，可用于客户搜索提醒（按得分高低排序）
 * @author uohzoaix
 *
 */
public class CBASearchHotJob {

	private static Jedis jedis = new Jedis("localhost");
	private static final String jedisKey = "hotKeyWord";
	private static final String jedisKeyNum = "hotKeyWordNum";
	private static Date date = new Date();

	public static class CBAMap extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (key.get() != 0) {// 过滤掉第一行，第一行为注释
				String log = value.toString();
				///newData3	1688407	/mobile/queryQuanPublishBySearch	[1688407,null,"message",null,null,2,20]	2013-11-14 08:43:03
				Matcher matcher = MapRedUtil.patternWithCorpIDAndArg.matcher(log);
				if (matcher.matches() && matcher.groupCount() == 5) {
					if (matcher.group(3).equals("/mobile/queryQuanPublishBySearch")) {
						String searchKeyWord = matcher.group(4).split(",")[3].replace("\"", "").trim();
						if (!"null".equalsIgnoreCase(searchKeyWord) && !"".equals(searchKeyWord) && searchKeyWord != null) {
							String dateTime = matcher.group(5);
							DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
							try {
								Date searchDate = format.parse(dateTime);
								Integer hour = (int) ((date.getTime() - searchDate.getTime()) / 1000 / 60 / 60);
								Integer length = searchKeyWord.length();//abcde
								searchKeyWord = searchKeyWord.substring(0, length <= 5 ? length : 5);
								for (int i = 0; i < searchKeyWord.length() - 1; i++) {//a ab abc abcd
									String subStr = searchKeyWord.substring(0, i + 1);
									//									Double num = jedis.zscore(jedisKeyNum, subStr);
									//									jedis.zadd(jedisKeyNum, num == null ? 1 : (num + 1), subStr);
									//Double score = jedis.zscore(jedisKey, subStr);
									jedis.zincrby(jedisKey, 1.0 / hour, subStr);
									//jedis.zadd(jedisKey, score == null ? 1.0 / hour : (score + 1.0 / hour), subStr);
								}
								//								Double num = jedis.zscore(jedisKeyNum, searchKeyWord);
								//								jedis.zadd(jedisKeyNum, num == null ? 1 : (num + 1), searchKeyWord);
								//Double score = jedis.zscore(jedisKey, searchKeyWord);
								jedis.zincrby(jedisKey, 1.0 / hour, searchKeyWord);
								//jedis.zadd(jedisKey, score == null ? 1.0 / hour : (score + 1.0 / hour), searchKeyWord);
								context.write(NullWritable.get(), NullWritable.get());
							} catch (ParseException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
	}

	//	public static class CBAReduce extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {
	//		private IntWritable result = new IntWritable();
	//
	//		@Override
	//		protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	//			int sum = 0;
	//			for (IntWritable val : values) {
	//				sum += val.get();
	//			}
	//			result.set(sum);
	//			context.write(key, result);
	//		}
	//	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CBASearchHotJob <in> <out>");
			System.exit(2);
		}
		jedis.zadd(jedisKey, 0, "");
		Job job = new Job(conf, "CBASearchHotJob");
		job.setJarByClass(CBASearchHotJob.class);
		job.setMapperClass(CBAMap.class);
		//job.setCombinerClass(CBACombine.class);
		//job.setReducerClass(CBAReduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
