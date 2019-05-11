package com.accenture.hadoop.count_product;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.avro.io.parsing.Symbol;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountUsersByProductTypeMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	
	protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		
		
		String lines = value.toString().trim();
		String[] fields = lines.split("\t");
		String[] Product=fields[5].split("\\&");
		String[] Product_key=Product[0].split("\\=");
		String Product_name=Product_key[1];
		if((Product_name.equals("eyewear"))){
			if(CountUsersByProductTypeDriver.customer_ip.contains(fields[4])){
				
			}
			else{
				
				CountUsersByProductTypeDriver.customer_ip.add(fields[4]);
				context.write(new Text(Product_name),new IntWritable(1));
			}
			
		}
		
		
		
		}
}
	

