package edu.boisestate.cs597;

/**
 * White house Map/Reduce
 *
 */

import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ProblemIII {

	//@TODO: Modify the contents of this class as appropriate
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

	}

	//@TODO: Modify the contents of this class as appropriate
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

	}

	public static void main(String[] args) throws Exception {

		//@TODO: Modify the contents of this method as appropriate
		
		GenericOptionsParser parser = new GenericOptionsParser(args);
		Configuration conf = parser.getConfiguration();

		String[] arguments = parser.getRemainingArgs();

		Job job = new Job(conf);
		job.setJarByClass(ProblemIII.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if (arguments.length < 3) {
			usage(System.out);
			System.exit(-1);
		}

		FileInputFormat.addInputPath(job, new Path(arguments[0]));
		FileInputFormat.addInputPath(job, new Path(arguments[1]));
		FileOutputFormat.setOutputPath(job, new Path(arguments[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static void usage(PrintStream out) {
		//@TODO: Modify the contents of this method as appropriate
		//@TODO: Print the usage and reason for exit
	}
}
