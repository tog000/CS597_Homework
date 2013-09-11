package edu.boisestate.cs597;

/**
 * White house Map/Reduce
 *
 */

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ProblemI {
	
	private static final Log LOG = LogFactory.getLog(ProblemI.class);
	
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
	
	public static class VisitorCount implements Comparable<VisitorCount> {
		public String visitor;
		public int count;
		public VisitorCount(String visitor, int count){this.visitor=visitor;this.count=count;}
		public int compareTo(VisitorCount vc) {
			return vc.count-this.count;
		};
		
	}
	
	public static class TopVisitorsReducer extends Reducer<LongWritable, Text, Text, IntWritable>{

		private static final int TOTAL_VISITORS = 20;
		
		private ArrayList<VisitorCount> topVisitors = new ArrayList<VisitorCount>();
		private Iterator<Text> iter;
		private VisitorCount vc;
		private int i;
		private String[] parts;
		private String line;
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			iter = values.iterator();
			
			parts = iter.next().toString().split("\t");
			// There should be only one value
			vc = new VisitorCount(parts[0]+"\t"+parts[1],Integer.valueOf(parts[2]).intValue());
			
			if(topVisitors.size() < TOTAL_VISITORS){
				topVisitors.add(vc);
			}else if(vc.count > topVisitors.get(0).count){
				// We have a new maximum
				topVisitors.add(0, vc);
				topVisitors.remove(TOTAL_VISITORS);
			}else if(vc.count > topVisitors.get(TOTAL_VISITORS-1).count){
				// We have something in between
				for(i=0;i<=TOTAL_VISITORS-1;i++){
					if(vc.count > topVisitors.get(i).count){
						topVisitors.add(i, vc);
						topVisitors.remove(TOTAL_VISITORS);
						break;
					}
				}
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			Iterator<VisitorCount> iter = topVisitors.iterator();
			while(iter.hasNext()){
				vc = iter.next();
				context.write(new Text(vc.visitor), new IntWritable(vc.count));
			}
		}
	}
	
	public static class BestWeekMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private String[] parts;
		private SimpleDateFormat sdf = new SimpleDateFormat("M/d/y H:m");
		private Calendar cal = Calendar.getInstance();
		private Date date;
		private int weekNumber;
		
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
			
			//FileSplit fileSplit =  (FileSplit) context.getInputSplit();
			
			parts = line.toString().split(",");
			
			if(parts.length > 12){
				try{
					// Try to parse the date
					date = sdf.parse(parts[12]);
				}catch(ParseException e){
					// Failed to parse date, return.
					return;
				}
			}else{
				// If for some reason the date is not there, return.
				return;
			}

			cal.setTime(date);
			
			// Generate key with week
			weekNumber = cal.get(Calendar.WEEK_OF_YEAR);

			context.write(new Text(Integer.valueOf(weekNumber).toString()), new IntWritable(1));
			
		}
		
	}
	
	public static class BestWeekReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		private int total = 0;
		private MultipleOutputs<Text, IntWritable> mos;
		
		public void setup(Context context) {
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}
		
		public void reduce(Text key, Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
			
			total=0;
			for(IntWritable count : values){
				total+=count.get();
			}
			
			// We flip the key so the sort step gets us the best week
			//context.write(key, new IntWritable(total));
			mos.write("weeks", key, new IntWritable(total));
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);	    
	    
		int QueryId = Integer.parseInt(args[2]);
		
		if ( QueryId == 1 ){
	    
			Job job1 = new Job(conf, "Extract top visitors");
			job1.setJarByClass(ProblemI.class);
	
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
	
			job1.setMapperClass(VisitorsMapper.class);
			job1.setCombinerClass(VisitorsReducer.class);
			job1.setReducerClass(VisitorsReducer.class);
	
			GenericOptionsParser gop = new GenericOptionsParser(args);
			String[] args2 = gop.getRemainingArgs();
			
			FileInputFormat.addInputPath(job1, new Path(args2[0]));
			
			Path tmpPath = new Path("tmp/");
			if(fs.exists(tmpPath)){
		    	fs.delete(tmpPath,true);
		    }
		    fs.deleteOnExit(tmpPath);
			
		    FileOutputFormat.setOutputPath(job1, tmpPath);
		    
		    Job job2 = new Job(conf, "Extract top 20 visitors");
		    job2.setJarByClass(ProblemI.class);
			job2.setReducerClass(TopVisitorsReducer.class);
			
			job2.setNumReduceTasks(1);
			
			FileInputFormat.addInputPath(job2, tmpPath);
			
			Path outputPath = new Path(args2[1]);
			if(fs.exists(outputPath)){
		    	fs.delete(outputPath,true);
		    }
			FileOutputFormat.setOutputPath(job2, outputPath);
			
			job1.waitForCompletion(true);
			job2.waitForCompletion(true);
			
		}else{
			
			Job job1 = new Job(conf, "Find best week");
			job1.setJarByClass(ProblemI.class);
		
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(IntWritable.class);
			
			job1.setMapperClass(BestWeekMapper.class);
			//job1.setCombinerClass(BestWeekReducer.class);
			job1.setReducerClass(BestWeekReducer.class);
	
			GenericOptionsParser gop = new GenericOptionsParser(args);
			String[] args2 = gop.getRemainingArgs();
			
			FileInputFormat.addInputPath(job1, new Path(args2[0]));
			
			Path tmpPath = new Path("tmp/");
			if(fs.exists(tmpPath)){
		    	fs.delete(tmpPath,true);
		    }
		    //fs.deleteOnExit(tmpPath);
			
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
			
		    FileOutputFormat.setOutputPath(job1, tmpPath);
		    MultipleOutputs.addNamedOutput(job1, "weeks", SequenceFileOutputFormat.class, Text.class, IntWritable.class);
		    
		    job1.waitForCompletion(true);
		    
		    
		    //
		    //
		    // TODO
		    // Map-Reduce by visitor, value = array of week numbers, save as a SequenceFileOutputFormat
		    // Set the input format to SequenceFileOutputFormat, Flip keys on the mapper, on the reducer write week and total visitors
		    // Read the file manually with the sequencefile reader
		    //
		    //
		    
		    
		    int[] maxWeek = {0,0};
		    FileStatus[] fss = fs.listStatus(tmpPath);
		    for (FileStatus status : fss) {
		    	if(status.getPath().getName().contains("weeks")){
			        Path path = status.getPath();
			        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			        Text key = new Text();
			        IntWritable value = new IntWritable();
			        while (reader.next(key, value)) {
			            if(value.get() > maxWeek[0]){
			            	maxWeek[0] = value.get();
			            	maxWeek[1] = Integer.valueOf(key.toString()); 
			            }
			        }
			        reader.close();
		    	}
		    }
		    
		    LOG.warn("Found the best week to be "+maxWeek[1]+" with "+maxWeek[0]+" visitors.");
		    
		    
		    /**
		    Job job2 = new Job(conf, "Extract top 20 visitors");
		    job2.setJarByClass(ProblemI.class);
			job2.setReducerClass(TopVisitorsReducer.class);
			
			job2.setNumReduceTasks(1);
			
			FileInputFormat.addInputPath(job2, tmpPath);
			
			Path outputPath = new Path(args2[1]);
			if(fs.exists(outputPath)){
		    	fs.delete(outputPath,true);
		    }
			FileOutputFormat.setOutputPath(job2, outputPath);
			job2.waitForCompletion(true);
			/**/	
		}
		
	}
}
