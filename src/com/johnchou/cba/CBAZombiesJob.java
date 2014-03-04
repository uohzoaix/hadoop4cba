package com.johnchou.cba;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 该类计算不活跃客户
 * @author user
 *
 */
public class CBAZombiesJob {

	private static List<String> files = new ArrayList<String>();

	public static class CBAMap extends Mapper<LongWritable, Text, Text, Text> {
		private final Text outkey = new Text();
		private final Text outvalue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			///newData	868557	/mobile/queryCorpMobileSubscribe		2013-11-12 08:09:25
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().getName();
			if (!files.contains(fileName))
				files.add(fileName);
			if (key.get() != 0) {// 过滤掉第一行，第一行为注释
				String log = value.toString();
				String[] logs = log.split("\t");
				if (logs.length >= 4 && !"".equals(log.split("\t")[1]) && !"NULL".equalsIgnoreCase(log.split("\t")[1]) && log.split("\t")[1] != null) {
					outkey.set(log.split("\t")[0] + "\t" + log.split("\t")[1]);
					outvalue.set(log.split("\t")[logs.length - 1]);
					context.write(outkey, outvalue);
				}
			}
		}
	}

	public static class CBAPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class CBAReduce extends Reducer<Text, Text, Text, Text> {
		private static final Text result = new Text();
		private static final Text outValue = new Text();
		private MultipleOutputs<Text, Text> outputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			outputs = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			outputs.close();
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Map<String, Integer>> clickPerDate = new HashMap<String, Map<String, Integer>>();
			StringBuffer sb = new StringBuffer();
			int count = 0;
			for (Text val : values) {
				count++;
				if (sb.length() > 0)
					sb.append(",");
				sb.append(val.toString());
			}
			if (count < files.size() * 3) {//每天点击次数小于3次认为是僵尸客户
				result.set(sb.toString());
				//context.write(key, result);
				//outputs.write(new Text(key.toString().split("\t")[0]), outValue, key.toString().split("\t")[0]);//写一行标识
				outputs.write(key, result, "/user/user/" + key.toString().split("\t")[0]);
			} else {
				//每天点击次数大于3次则判断每天的点击时间分布情况，如果分布不均匀（比如上午下午各个时间段都有点击）则任务是活跃客户，否则也认为是僵尸客户
				//2013-11-14 10:11:37,2013-11-14 10:11:43,2013-11-14 10:11:33,2013-11-14 10:11:26,2013-11-14 11:25:48,2013-11-14 11:25:48,2013-11-14 10:39:07,2013-11-14 10:39:07,2013-11-12 08:11:45,2013-11-12 08:11:47
				String[] clickDTs = sb.toString().split(",");
				for (String clickDT : clickDTs) {
					String clickD = clickDT.split(" ")[0];
					String clickT = clickDT.split(" ")[1].substring(0, 2);
					Map<String, Integer> clickPerHour = clickPerDate.get(clickD);
					if (clickPerHour != null) {
						Integer clickNum = clickPerHour.get(clickT);
						clickPerHour.put(clickT, clickNum == null ? 1 : (clickNum + 1));
					} else {
						clickPerHour = new HashMap<String, Integer>();
						clickPerHour.put(clickT, 1);
						clickPerDate.put(clickD, clickPerHour);
					}
				}
				boolean zoombies = true;
				if (key.toString().indexOf("1246208") >= 0) {
					System.out.println(key);
				}
				labelouter: for (Map.Entry<String, Map<String, Integer>> entryDate : clickPerDate.entrySet()) {
					Map<String, Integer> clickPerHour = entryDate.getValue();
					//如果每天都只有1个时间段点击则认为是僵尸客户
					if (entryDate.getValue().size() > 1) {
						//zoombies = false;
						for (Map.Entry<String, Integer> entryHour : clickPerHour.entrySet()) {
							if (entryHour.getValue() < 3) {
								//entryHour.getValue()<3表示一个时间段内只点击了2次
								zoombies = true;
							} else {
								//只要有一个时间段的点击数大于3次就认为是活跃客户
								zoombies = false;
								break labelouter;
							}
						}
					} else {
						if (Integer.valueOf(clickPerHour.values().toArray()[0].toString()) > 5) {
							//如果只有一个时间段点击但是该时间段的点击数大于5也认为是活跃客户
							zoombies = false;
							break labelouter;
						} else {
							zoombies = true;
						}
					}
				}
				if (zoombies) {
					result.set(sb.toString());
					//context.write(key, result);
					//outputs.write(new Text(key.toString().split("\t")[0]), outValue, key.toString().split("\t")[0]);
					outputs.write(key, result, "/user/user/" + key.toString().split("\t")[0]);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CBAZombiesJob <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "CBAZombiesJob");
		job.setJarByClass(CBAZombiesJob.class);
		job.setMapperClass(CBAMap.class);
		//job.setPartitionerClass(CBAPartitioner.class);
		job.setReducerClass(CBAReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//System.out.println(("newData3".hashCode()& Integer.MAX_VALUE) % 2);
	}
}
