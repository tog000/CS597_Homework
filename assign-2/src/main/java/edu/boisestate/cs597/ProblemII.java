package edu.boisestate.cs597;

/**
 * Matrix Multiplication
 *
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class ProblemII {

	//@TODO: Change the Key/Value types of the Map to suite your problem definition 
	public static class Map extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		public void map(Text arg0, Text arg1, OutputCollector<Text, Text> arg2,
				Reporter arg3) throws IOException {
			// TODO Auto-generated method stub

		}
	}

	//@TODO: Change the Key/Value types of the Reduce to suite your problem definition
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text arg0, Iterator<Text> arg1,
				OutputCollector<Text, Text> arg2, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			
		}
	}

	public static void main(String[] args) throws Exception {
		
		//@TODO: Change the contents of this method suite your problem definition
		JobConf conf = new JobConf(ProblemII.class);
		conf.setJobName("Problem II");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		//@TODO modify this section to use GenericOptionsParser
		// See : http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/util/GenericOptionsParser.html
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		//Find out how to read matrix B from HDFS file system.It is passed in args[1]
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		

		JobClient.runJob(conf);
	}
}
