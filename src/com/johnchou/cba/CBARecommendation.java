package com.johnchou.cba;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 该类用于实现商家之间的推荐，输入文件为subscribeandclick
 * @author uohzoaix
 *
 */
public class CBARecommendation {

	private static Map<String, String> corpID2ProductID = new HashMap<String, String>();
	public static Map<String, Double> weightMap = new HashMap<String, Double>();
	//各个品种的所有点击数量和
	public static Map<String, Integer> freqMap = new HashMap<String, Integer>();

	public static class CBAMap extends Mapper<LongWritable, Text, Text, Text> {
		private final static Text corpID = new Text();
		private final static Text productIDAndClicks = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String log[] = value.toString().split("\t");
			corpID.set(log[0]);
			productIDAndClicks.set(log[1] + "=" + log[2]);
			if (freqMap.containsKey(log[1])) {
				freqMap.put(log[1], freqMap.get(log[1]) + Integer.valueOf(log[2]));
			} else {
				freqMap.put(log[1], Integer.valueOf(log[2]));
			}
			//System.out.println(productIDAndClicks.toString());
			context.write(corpID, productIDAndClicks);
		}
	}

	/**
	 * @author user
	 * 
	 */
	public static class CBACombine extends Reducer<Text, Text, Text, Text> {
		private final static Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text val : values) {
				sb.append(val + ",");
			}
			String productIDAndClick = sb.deleteCharAt(sb.length() - 1).toString();
			corpID2ProductID.put(key.toString(), productIDAndClick);//corpID-->productid=clicknum
			result.set(productIDAndClick);
			context.write(key, result);
		}

	}

	public static class CBAReduce extends Reducer<Text, Text, Text, Text> {
		private final static Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text val : values) {
				sb.append(val + ",");
			}
			for (Entry<String, String> entry : corpID2ProductID.entrySet()) {
				if (!key.toString().equals(entry.getKey())) {
					String productIDAndClicks = entry.getValue();
					int xLength = sb.toString().split(",").length;//自己的点击品种和数量
					int yLength = productIDAndClicks.split(",").length;//别人的点击品种和数量
					if (xLength != 0 && yLength != 0) {
						double intersectionWeight = 0;
						double sourceWeight = 0;
						double targetWeight = 0;
						//计算自己的点击品种的权重
						String xProductIDAndClicks[] = sb.toString().split(",");
						List<String> xProductIDs = new ArrayList<String>();
						for (String xProductIDAndClick : xProductIDAndClicks) {
							xProductIDs.add(xProductIDAndClick.split("=")[0]);
							sourceWeight += (double) 1 / (Math.log(freqMap.get(xProductIDAndClick.split("=")[0])) + 1);
						}
						//计算别人的点击品种的权重
						String yProductIDAndClicks[] = productIDAndClicks.split(",");
						List<String> yProductIDs = new ArrayList<String>();
						for (String yProductIDAndClick : yProductIDAndClicks) {
							yProductIDs.add(yProductIDAndClick.split("=")[0]);
							targetWeight += (double) 1 / (Math.log(freqMap.get(yProductIDAndClick.split("=")[0])) + 1);
						}
						//计算共同点击品种的权重
						List<String> intersection = intersection(xProductIDs, yProductIDs);
						for (String inString : intersection) {
							intersectionWeight += (double) 1 / (Math.log(freqMap.get(inString)) + 1);
						}
						//采用jaccard distance计算距离
						double distance = 1 - 2 * intersectionWeight / (sourceWeight + targetWeight);
						if (distance <= 0.5) {//商家距离小于0.5
							yProductIDs.removeAll(intersection);
							if(yProductIDs.size()!=0){
								result.set(entry.getKey() + "\t" + yProductIDs);
								System.out.println(result.toString());
								context.write(key, result);
							}
						}
					}
				}
			}
		}

	}

	public static void JaccardWeight(String productIDAndClicks) {

		for (String string : productIDAndClicks.split(",")) {
			String[] ss = string.split("=");
			if (freqMap.containsKey(ss[0])) {
				freqMap.put(ss[0], freqMap.get(ss) + Integer.valueOf(ss[1]));
			} else {
				freqMap.put(ss[0], Integer.valueOf(ss[1]));
			}
		}
	}

	public static List<String> intersection(List<String> slist, List<String> tlist) {
		List<String> intersection = new ArrayList<String>();
		for (String s : slist) {
			if (tlist.contains(s)) {
				if (!intersection.contains(s)) {
					intersection.add(s);
				}
			}
		}
		return intersection;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CBAHitsByApp <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "CBARecommendation");
		job.setJarByClass(CBARecommendation.class);
		job.setMapperClass(CBAMap.class);
		job.setCombinerClass(CBACombine.class);
		job.setReducerClass(CBAReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
