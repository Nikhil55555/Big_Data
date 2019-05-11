package com.accenture.hadoop.count_timespent;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.avro.io.parsing.Symbol;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountUsersTimeSpentTypeMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	
	protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		
		
		String lines = value.toString().trim();
		String[] fields = lines.split("\t");
		int timespent = Integer.parseInt(fields[6]);
		context.write(new Text(fields[4]),new IntWritable(timespent));
			
			
		
		
		
		
		}
}