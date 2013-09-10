package edu.boisestate.cs597;

/**
 * White house Map/Reduce
 *
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ProblemI {
	
	public static class VisitorsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private String[] parts;
		
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
			
			//FileSplit fileSplit =  (FileSplit) context.getInputSplit();
			
			parts = line.toString().split(",");
			
			if(parts.length > 2){
				context.write(new Text(parts[0].toUpperCase()+"\t"+parts[1].toUpperCase()), new IntWritable(1));
			}
		}
		
	}
	
	public static class VisitorsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		private int total = 0;
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			total=0;
			for(IntWritable count : values){
				total+=count.get();
			}
			
			context.write(key, new IntWritable(total));
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
		
		Job job1 = new Job(conf, "Problem I");
		job1.setJarByClass(ProblemI.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setMapperClass(VisitorsMapper.class);
		job1.setCombinerClass(VisitorsReducer.class);
		job1.setReducerClass(VisitorsReducer.class);

		GenericOptionsParser gop = new GenericOptionsParser(args);
		String[] args2 = gop.getRemainingArgs();
		
		FileInputFormat.addInputPath(job1, new Path(args2[0]));
		
		Path outputPath = new Path(args2[1]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		FileOutputFormat.setOutputPath(job1, outputPath);
		
		
		job1.waitForCompletion(true);
		
		int QueryId = Integer.parseInt(args[2]);

		
		
	}
}
