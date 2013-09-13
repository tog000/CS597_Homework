package edu.boisestate.cs597;

/**
 * White house Map/Reduce
 *
 */

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ProblemIII {

	public static final String NODES_FILE = "nfile";
	
	public static final String SOURCE_PROTEIN_NAME = "src";
	public static final String DESTINATION_PROTEIN_NAME = "dst";
	
	public static final String SOURCE_PROTEIN_NUMBER = "srcn";
	public static final String DESTINATION_PROTEIN_NUMBER = "dstn";
	
	//@TODO: Modify the contents of this class as appropriate
	public static class Map extends Mapper<LongWritable, Text, ProteinWritable, ProteinWritable> {
		
		private String source;
		private String destination;
		private String currentLine;
		private Scanner scn;
		private boolean isNodesFile;
		private ProteinWritable protein;
		private ProteinWritable protein2;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			
			Configuration conf = context.getConfiguration();
			source = conf.get(ProblemIII.SOURCE_PROTEIN_NAME);
			destination = conf.get(ProblemIII.DESTINATION_PROTEIN_NAME);
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().toString();
			
			isNodesFile = false;
			if(filename.endsWith(conf.get(ProblemIII.NODES_FILE))){
				isNodesFile = true;
			}
			
		}
		
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
			
			currentLine = line.toString();
			
			scn = new Scanner(currentLine);
			
			if(isNodesFile){
				
				protein = new ProteinWritable(scn.nextInt(),scn.next().replace("\"", "")); 
				
				context.write(protein, protein);
				
				if(currentLine!=null && currentLine.contains(source)){
					
					context.getConfiguration().setInt(ProblemIII.SOURCE_PROTEIN_NUMBER,protein.id.get());
					System.out.println("FOUND THE SOURCE -> "+protein.id.get());
					
				}else if(currentLine!=null && currentLine.contains(destination)){
					
					context.getConfiguration().setInt(ProblemIII.DESTINATION_PROTEIN_NUMBER,protein.id.get());
					System.out.println("FOUND THE DESTINATION -> "+protein.id.get());
					
				}
			}else{
				protein = new ProteinWritable(scn.nextInt(),"NONE");
				protein2 = new ProteinWritable(scn.nextInt(),"");
				context.write(protein, protein2);
			}

		}
		
	}
	
	public static class ProteinComparator extends WritableComparator {
		protected ProteinComparator() {
			super(ProteinWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			ProteinWritable p1 = (ProteinWritable) w1;
			ProteinWritable p2 = (ProteinWritable) w2;
			
			return p1.id.compareTo(p2.id);
		}
	}

	public static class Reduce extends Reducer<ProteinWritable, ProteinWritable, Text, Text> {

		Iterator<ProteinWritable> i;
		
		@Override
		public void reduce(ProteinWritable key, Iterable<ProteinWritable> values, Context context) throws IOException, InterruptedException{
			
			i=values.iterator();
			ProteinWritable pw = i.next();
			context.write(pw.name,new Text("---> "+pw.id.toString()));

		}
		
	}

	public static void main(String[] args) throws Exception {

		//@TODO: Modify the contents of this method as appropriate
		
		GenericOptionsParser parser = new GenericOptionsParser(args);
		Configuration conf = parser.getConfiguration();

		FileSystem fs = FileSystem.get(conf);
		
		String[] arguments = parser.getRemainingArgs();

		conf.set(NODES_FILE, arguments[0]);
		
		conf.set(SOURCE_PROTEIN_NAME, arguments[3]);
		conf.set(DESTINATION_PROTEIN_NAME, arguments[4]);
		
		Job job = new Job(conf,"Resolve protein name to node number");
		job.setJarByClass(ProblemIII.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setGroupingComparatorClass(ProteinComparator.class);
		
		job.setMapOutputKeyClass(ProteinWritable.class);
		job.setMapOutputValueClass(ProteinWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if (arguments.length < 5) {
			usage(System.out);
			System.exit(-1);
		}

		FileInputFormat.addInputPath(job, new Path(arguments[0]));
		FileInputFormat.addInputPath(job, new Path(arguments[1]));
		
		Path outputPath = new Path(arguments[2]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static void usage(PrintStream out) {
		out.println("Usage: <path to nodes.txt> <path to edges.txt> <path to output.txt> <source protein name> <destination protein name>");
	}
}
