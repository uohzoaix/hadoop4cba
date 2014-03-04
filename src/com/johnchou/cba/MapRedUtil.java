package com.johnchou.cba;

import java.util.regex.Pattern;

public class MapRedUtil {

	/**
	 * group(1):app group(2):corpID group(3):path group(4):args group(5):datetime
	 */
	// 有corpID有参正则
	public static final Pattern patternWithCorpIDAndArg = Pattern.compile("(/[^\\s]+)\t([0-9]+)\t(/[^\\s]*)\t\\[(.+)\\]\t(.+)");
	// 有corpID无参正则
	public static final Pattern patternWithCorpIDButArg = Pattern.compile("(/[^\\s]+)\t([0-9]+)\t(/[^\\s]*)\t(.+)");
	// 无corpID有参正则
	public static final Pattern patternWithoutCorpIDButArg = Pattern.compile("(/[^\\s]+)\t\t(/[^\\s]*)\t\\[(.+)\\]\t(.+)");
	// 无corpID无参正则
	public static final Pattern patternWithoutCorpIDAndArg = Pattern.compile("(/[^\\s]+)\t\t(/[^\\s]*)\t\t(.+)");
	// 可适合各种情况正则
	public static final Pattern allPattern = Pattern.compile("(/[^\\s]+)\t(.*)\t(/[^\\s]*)\t(.*)\t(.+)");
}
