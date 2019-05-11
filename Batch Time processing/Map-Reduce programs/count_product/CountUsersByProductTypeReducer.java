package com.accenture.hadoop.count_product;



import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountUsersByProductTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	protected void reduce(Text key, Iterable<IntWritable> value, Context context)throws IOException, InterruptedException {
	
		int sum = 0;
		
		for (IntWritable i : value) {
		sum = sum + i.get();
		}
		context.write(key, new IntWritable(sum));
	}
	
}