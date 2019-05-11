package com.accenture.hadoop.min_timespent;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CountUsersMinTimeSpentTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public static int min= 1554695;
	public static Text ip=new Text(" ");
	

	protected void reduce(Text key, Iterable<IntWritable> value, Context context)throws IOException, InterruptedException {
	
		int sum = 0;
		
		for (IntWritable i : value) {
		sum = sum + i.get();
		}
		if(sum<min){
			min=sum;
			ip=new Text(key);
		}
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(ip), new IntWritable(min)); 
		}
	
}