package com.accenture.hadoop.count_product;


import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountUsersByProductTypeDriver {
	public static ArrayList<String> customer_ip = new ArrayList<String>();

	public static void main(String[] args) throws Exception {
		// creating the configuration
		/* comment the below 3 lines to make input path and output path parameterized from command line arguments while building as jar */
		
		
		
		args= new String[3];
		args[0]="C:\\Users\\training_b6b.01.03\\Desktop\\hadoop\\goShopping_WebClicks.dat";
		args[1]="C:\\Users\\training_b6b.01.03\\Desktop\\hadoop\\MR002_OutPut";
		
		Configuration conf = new Configuration();
		// creating the job instance
		Job job = Job.getInstance(conf, "CountUsersByCountryType");

		// set the Mapper, Reducer, Driver details to job
		job.setJarByClass(CountUsersByProductTypeDriver.class);
		job.setMapperClass(CountUsersByProductTypeMapper.class);
		job.setReducerClass(CountUsersByProductTypeReducer.class);
		// set the map & reduce output key,value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// set the file input and output paths
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// job submission
		boolean jobStatus = job.waitForCompletion(true);
		if (jobStatus == false) {
			System.exit(1);
		}
	}
	
}

