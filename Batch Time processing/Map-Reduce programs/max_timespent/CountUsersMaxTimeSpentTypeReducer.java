package com.accenture.hadoop.max_timespent;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CountUsersMaxTimeSpentTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public static int max= 0;
	public static Text ip=new Text(" ");
	

	protected void reduce(Text key, Iterable<IntWritable> value, Context context)throws IOException, InterruptedException {
	
		int sum = 0;
		
		for (IntWritable i : value) {
		sum = sum + i.get();
		}
		if(max<sum){
			max=sum;
			ip=new Text(key);
			
		}
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(ip), new IntWritable(max)); 
		}
	
}