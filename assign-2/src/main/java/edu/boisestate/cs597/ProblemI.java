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
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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
				context.write(new Text(parts[1].toUpperCase()+"\t"+parts[0].toUpperCase()), new IntWritable(1));
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
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			iter = values.iterator();
			
			parts = iter.next().toString().split("\t");
			// There should be only one value
			vc = new VisitorCount(parts[0]+"\t"+parts[1],Integer.valueOf(parts[2]).intValue());
			
			if(topVisitors.size() < TOTAL_VISITORS){
				topVisitors.add(vc);
				if(topVisitors.size() == TOTAL_VISITORS){
					Collections.sort(topVisitors);
				}
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
	
	public static class BestWeekMapper extends Mapper<LongWritable, Text, Text, VisitWritable>{
		
		private String[] parts;
		private SimpleDateFormat sdf = new SimpleDateFormat("M/d/y H:m");
		private Calendar cal = Calendar.getInstance();
		private Date date;
		private int weekNumber;
		private String visitor;
		private String visitee;
		
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
			
			parts = line.toString().split(",");
			
			if(parts.length > 20){
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
			
			weekNumber = cal.get(Calendar.WEEK_OF_YEAR);
			visitor = (parts[1]+"\t"+parts[0]).toUpperCase();
			visitee = (parts[20]+"\t"+parts[19]).toUpperCase();

			// THIS DOESN'T WORK, we want UNIQUE visitors
			//context.write(new IntWritable(weekNumber),new VisitWritable(weekNumber, visitor, visitee));

			// Create custom object for storing visitor/visitee and write to context
			context.write(new Text(visitor),new VisitWritable(weekNumber, visitor, visitee));
			
			
		}
		
	}
	
	public static class BestWeekReducer extends Reducer<Text, VisitWritable, IntWritable, VisitWritable>{
		
		private boolean weeks[] = new boolean[54];
		
		public void reduce(Text key, Iterable<VisitWritable> values,  Context context) throws IOException, InterruptedException {
			
			// We want write back with the week as the key and VisitWritable as value, keeping track of weeks to remove duplicates
			for(VisitWritable visit : values){
				if(!weeks[visit.week.get()]){
					context.write(visit.week, visit);
				}
			}			
		}
	}
	
	public static class BestWeekVisiteesReducer extends Reducer<IntWritable, VisitWritable, Text, Text>{
		
		// I have two different outputs for the files, can't parametrize MultipleOutputs.
		private MultipleOutputs mos;
		
		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}

		public void reduce(IntWritable key, Iterable<VisitWritable> values,  Context context) throws IOException, InterruptedException {
			
			// We want write back with the visitee as key and visitor as value
			int total = 0;
			for(VisitWritable visit : values){
				mos.write("week", visit.visitee, visit.visitor, key.toString());
				total += 1;
			}

			// Write to the totals file
			mos.write("weekTotals", key, new IntWritable(total));
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
	}

//	private class BestWeekPathFilter implements PathFilter{
//		public BestWeekPathFilter() {}
//		public boolean accept(Path path) {
//			return path.getName().contains("-BEST");
//		}
//	}
	
	public static class BestWeekVisiteesCountReducer extends Reducer<Text, Text, TextIntWritable, NullWritable>{
		
		public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {
			
			// We want write back with the visitee and the count
			int total = 0;
			for(@SuppressWarnings("unused") Text visitor : values){
				total += 1;
			}
			context.write(new TextIntWritable(key,total), NullWritable.get());
		}
	}
	
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(TextIntWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextIntWritable tw1 = (TextIntWritable) w1;
			TextIntWritable tw2 = (TextIntWritable) w2;
			int cmp = -tw1.number.compareTo(tw2.number);
			// If the numbers are different, sort ASC by number
			if (cmp != 0) {
				return cmp;
			}
			// Otherwise, sort alphabetically
			return tw1.text.compareTo(tw2.text);
		}
	}

	public static class SortedVisiteesReducer extends Reducer<TextIntWritable, NullWritable, Text, IntWritable>{
		
		public void reduce(TextIntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			
			// We want write back with the visitee and the count
			context.write(key.text,key.number);
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
			
			Job job1 = new Job(conf, "Write all pairs for every week");
			job1.setJarByClass(ProblemI.class);
			
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(VisitWritable.class);
			job1.setMapperClass(BestWeekMapper.class);			
			
			job1.setOutputKeyClass(IntWritable.class);
			job1.setOutputValueClass(VisitWritable.class);
			job1.setReducerClass(BestWeekReducer.class);
			
			// If we output the SequenceFileOutputFormat, the next mapper won't need to parse
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			GenericOptionsParser gop = new GenericOptionsParser(args);
			String[] args2 = gop.getRemainingArgs();
			
			FileInputFormat.addInputPath(job1, new Path(args2[0]));
			
			Path tmpPath = new Path("tmp/");
			if(fs.exists(tmpPath)){
		    	fs.delete(tmpPath,true);
		    }
		    fs.deleteOnExit(tmpPath);
			
		    FileOutputFormat.setOutputPath(job1, tmpPath);
		    
		    job1.waitForCompletion(true);
		    
		    Job job2 = new Job(conf, "Find best week");
			job2.setJarByClass(ProblemI.class);
		
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(VisitWritable.class);
			
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			job2.setReducerClass(BestWeekVisiteesReducer.class);

			// Custom output file for week totals 
			MultipleOutputs.addNamedOutput(job2, "weekTotals", SequenceFileOutputFormat.class, IntWritable.class, IntWritable.class);

			// Additional files for the weekly results
			MultipleOutputs.addNamedOutput(job2, "week", SequenceFileOutputFormat.class, Text.class, Text.class);
			

			FileInputFormat.addInputPath(job2, tmpPath);

			Path tmpPath2 = new Path("tmp2/");
			if(fs.exists(tmpPath2)){
		    	fs.delete(tmpPath2,true);
		    }
		    fs.deleteOnExit(tmpPath2);
			
			FileOutputFormat.setOutputPath(job2, tmpPath2);
		    
		    job2.waitForCompletion(true);

		    // Find the best week from the totals file
		    int[] weekTotals = new int[54];
		    Path path;
		    // For every file in the output directory
		    FileStatus[] fss = fs.listStatus(tmpPath2);
		    for (FileStatus status : fss) {
		    	// There might be multiple weekTotals if there are multiple reducers, so we accumulate totals
		    	if(status.getPath().getName().contains("weekTotals")){
			        path = status.getPath();
			        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			        IntWritable key = new IntWritable();
			        IntWritable value = new IntWritable();
			        while (reader.next(key, value)) {
			        	weekTotals[key.get()] += value.get();
			        }
			        reader.close();
		    	}
		    }
		    
		    // Find the maximum accumulated total for a week
		    int[] maxWeek = new int[2];
		    for(int i=0;i<weekTotals.length;i++){
		    	if(weekTotals[i] > maxWeek[0]){
	            	maxWeek[0] = weekTotals[i];
	            	maxWeek[1] = i; 
	            }
		    }
		    
		    LOG.warn("Found the best week to be "+maxWeek[1]+" with "+maxWeek[0]+" visitors.");
		    
		    // Add the files for the right week to a list that later will be fed to the FileInputFormat
		    LinkedList<Path> bestWeekFiles = new LinkedList<Path>();
		    for (FileStatus status : fss) {
		    	if(status.getPath().getName().contains(Integer.valueOf(maxWeek[1]).toString()+"-")){
			        path = status.getPath();
			        
			        // PathFilter doesn't work. Why? No clue.
			        
			        //Path bestWeek = new Path(tmpPath2.getName()+"/"+path.getName()+"-BEST");
				    //fs.rename(path, bestWeek);
				    //LOG.warn("Renamed file: "+path.toString()+" to "+bestWeek.toString());
			        
				    bestWeekFiles.add(path);
		    	}
		    }
		    
		    // Accumulate the totals from the visitors for the week
		    Job job3 = new Job(conf, "Find list of visitees week");
			job3.setJarByClass(ProblemI.class);
			
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);
			
			job3.setInputFormatClass(SequenceFileInputFormat.class);
			job3.setOutputFormatClass(SequenceFileOutputFormat.class);
		
			job3.setOutputKeyClass(TextIntWritable.class);
			job3.setOutputValueClass(NullWritable.class);
			
			job3.setReducerClass(BestWeekVisiteesCountReducer.class);
			
			for(Path p : bestWeekFiles){
				FileInputFormat.addInputPath(job3, p);
			}
			
			Path tmpPath3 = new Path("tmp3/");
			if(fs.exists(tmpPath3)){
		    	fs.delete(tmpPath3,true);
		    }
		    fs.deleteOnExit(tmpPath3);
			
			FileOutputFormat.setOutputPath(job3, tmpPath3);
		    
		    job3.waitForCompletion(true);
		    
		    // Secondary sort
		    Job job4 = new Job(conf, "Secondary Sort");
			job4.setJarByClass(ProblemI.class);
			
			job4.setMapOutputKeyClass(TextIntWritable.class);
			job4.setMapOutputValueClass(NullWritable.class);
			
		    job4.setSortComparatorClass(KeyComparator.class);
		    
		    job4.setReducerClass(SortedVisiteesReducer.class);
		    
		    job4.setInputFormatClass(SequenceFileInputFormat.class);
		    
		    Path outputPath = new Path(args2[1]);
			if(fs.exists(outputPath)){
		    	fs.delete(outputPath,true);
		    }
		    
			FileInputFormat.addInputPath(job4, tmpPath3);
			FileOutputFormat.setOutputPath(job4, outputPath);

		    job4.waitForCompletion(true);
			
		}
		
	}
}
