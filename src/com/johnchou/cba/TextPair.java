package com.johnchou.cba;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {

	/**
	 * the first field
	 */
	Text first;

	/**
	 * the second field
	 */
	Text second;

	public TextPair() {

	}

	public TextPair(Text t1, Text t2) {
		first = t1;
		second = t2;
	}

	/**
	 * set the first Text
	 * 
	 * @param t1
	 */
	public void setFirstText(String t1) {
		first.set(t1.getBytes());
	}

	/**
	 * set the second text
	 * 
	 * @param t2
	 */
	public void setSecondText(String t2) {
		second.set(t2.getBytes());
	}

	/**
	 * get the first field
	 * 
	 * @return the first field
	 */
	public Text getFirst() {
		return first;
	}

	/**
	 * get the second field
	 * 
	 * @return the second field
	 */
	public Text getSecond() {
		return second;
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		if (first == null)
			first = new Text();

		if (second == null)
			second = new Text();

		first.readFields(in);
		second.readFields(in);
	}

	public int hashCode() {
		return first.hashCode();
	}

	public boolean equals(Object o) {
		TextPair p = (TextPair) o;
		return first.equals(p.getFirst());
	}

	public String toString() {
		return first + "\t" + second;
	}

	@Override
	public int compareTo(TextPair o) {
		TextPair ip2 = (TextPair) o;
		Integer flen1 = getFirst().getLength();
		Integer flen2 = ip2.getFirst().getLength();
		int flenCmp = flen1.compareTo(flen2);
		if (flenCmp == 0) {
			int cmp = getFirst().compareTo(ip2.getFirst());
			if (cmp != 0) {
				return cmp;
			}
			Integer slen1 = getSecond().getLength();
			Integer slen2 = ip2.getSecond().getLength();
			int slenCmp = slen1.compareTo(slen2);
			if (slenCmp == 0) {
				return getSecond().compareTo(ip2.getSecond()); // reverse
			} else {
				return slenCmp;
			}
		}
		return flenCmp;
	}
}
