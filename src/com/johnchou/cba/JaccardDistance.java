package com.johnchou.cba;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JaccardDistance {
	public static Map<String, Double> weightMap = new HashMap<String, Double>();

	/**
	 * intersection between two strings
	 * @param source
	 * @param target
	 * @return
	 */
	public static List<String> intersection(String source, String target) {
		List<String> slist = Arrays.asList(source.split(" "));
		List<String> tlist = Arrays.asList(target.split(" "));
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

	/**
	 * J(s,t) = 1 - intersection(s, t).size()) / (s.size() + t.size() - intersection.size())	 * 
	 * @param source
	 * @param target
	 */
	public static double Jaccard1(String source, String target) {
		List<String> slist = Arrays.asList(source.split(" "));
		List<String> tlist = Arrays.asList(target.split(" "));
		List<String> intersection = intersection(source, target);

		return (double) 1 - intersection.size() / (double) (slist.size() + tlist.size() - intersection.size());
	}

	/**
	 * J(s,t) = 1 - 2 * intersection(s, t).size()) / (s.size() + t.size())
	 * @param source
	 * @param target
	 * @return
	 */
	public static double Jaccard2(String source, String target) {
		List<String> slist = Arrays.asList(source.split(" "));
		List<String> tlist = Arrays.asList(target.split(" "));
		List<String> intersection = intersection(source, target);

		return (double) 1 - 2 * intersection.size() / (double) (slist.size() + tlist.size());
	}

	/**
	 * J(s,t) each token has weight value.
	 * @param stringList
	 * @param token
	 * @return
	 */
	public static void JaccardWeight(List<String> stringList) {
		Map<String, Integer> freqMap = new HashMap<String, Integer>();

		for (String string : stringList) {
			List<String> slist = Arrays.asList(string.split(" "));
			for (String s : slist) {
				s = s.trim();
				if (freqMap.containsKey(s)) {
					freqMap.put(s, freqMap.get(s) + 1);
				} else {
					freqMap.put(s, 1);
				}
			}
		}

		for (String key : freqMap.keySet()) {
			int freq = freqMap.get(key);
			double weight = (double) 1 / (Math.log(freq) + 1);
			weightMap.put(key, weight);
		}
		//		return weightMap;		
	}

	public static double Jaccard3(String source, String target) {
		List<String> slist = Arrays.asList(source.split(" "));
		List<String> tlist = Arrays.asList(target.split(" "));
		List<String> intersection = intersection(source, target);

		double intersectionWeight = 0;
		double sourceWeight = 0;
		double targetWeight = 0;
		for (String s : intersection) {
			intersectionWeight += weightMap.get(s);
		}
		for (String s : slist) {
			sourceWeight += weightMap.get(s);
		}
		for (String s : tlist) {
			targetWeight += weightMap.get(s);
		}

		return 1 - 2 * intersectionWeight / (sourceWeight + targetWeight);
	}

	//main
	public static void main(String[] args) {
		String s1 = "AAE HOLDING";
		String s2 = "AAE TECHNOLOGY INTERNATIONAL";
		String s3 = "AGRIPA HOLDING";
		System.out.println("J1(s1, s2) = " + Jaccard1(s1, s2));
		System.out.println("J1(s1, s3) = " + Jaccard1(s1, s3));
		System.out.println("J1(s2, s3) = " + Jaccard1(s2, s3));

		System.out.println();
		System.out.println("J2(s1, s2) = " + Jaccard2(s1, s2));
		System.out.println("J2(s1, s3) = " + Jaccard2(s1, s3));
		System.out.println("J2(s2, s3) = " + Jaccard2(s2, s3));

		System.out.println();
		List<String> stringList = new ArrayList<String>();
		Collections.addAll(stringList, s1, s2, s3);
		JaccardWeight(stringList);
		System.out.println(weightMap);
		System.out.println("J3(s1, s2) = " + Jaccard3(s1, s2));
		System.out.println("J3(s1, s3) = " + Jaccard3(s1, s3));
		System.out.println("J3(s2, s3) = " + Jaccard3(s2, s3));
	}

}
