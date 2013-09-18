package edu.boisestate.cs597;

/**
 * White house Map/Reduce
 *
 */

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ProblemIII {

	public static final String OUTPUT_FILE = "ofile";
	
	public static final String NODES_FILE = "nfile";
	public static final String EDGES_FILE = "efile";
	
	public static final String SOURCE_PROTEIN_NAME = "src";
	public static final String DESTINATION_PROTEIN_NAME = "dst";
	
	public static final String SOURCE_PROTEIN_NUMBER = "srcn";
	public static final String DESTINATION_PROTEIN_NUMBER = "dstn";
	
	public static final String FROM_EDGES = "BBB";
	public static final String FROM_GRAPH = "AAA";
	
	public enum MyCounter{
		  ITERATION_COUNT,
		  CONVERGENCE_COUNTER,
		  DESTINATION_FOUND_COUNTER
	}	
	
	public static class Map extends Mapper<LongWritable, Text, ProteinWritable, ProteinWritable> {
		
		private String source;
		private String destination;
		private String currentLine;
		private Scanner scn;
		private boolean isNodesFile;
		private ProteinWritable protein;
		private ProteinWritable protein2;
		private MultipleOutputs mos;
		
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
			
			mos = new MultipleOutputs(context);
			
		}
		
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
			
			currentLine = line.toString();
			
			scn = new Scanner(currentLine);
			
			if(isNodesFile){
				
				protein = new ProteinWritable(scn.nextInt(),scn.next().replace("\"", ""));				
				mos.write("nodes", protein, protein);
				
				context.write(protein, protein);
				
				if(currentLine!=null && currentLine.contains(source)){
					context.getConfiguration().setInt(ProblemIII.SOURCE_PROTEIN_NUMBER,protein.id.get());
					mos.write("source", protein.id, NullWritable.get());
				}else if(currentLine!=null && currentLine.contains(destination)){
					context.getConfiguration().setInt(ProblemIII.DESTINATION_PROTEIN_NUMBER,protein.id.get());
					mos.write("destination", protein.id, NullWritable.get());
				}
				
			}else{
				protein = new ProteinWritable(scn.nextInt(),"");
				protein2 = new ProteinWritable(scn.nextInt(),"");
				System.out.println("FOUND AN EDGE-> "+protein2.id.get());
				
				mos.write("edges", protein, protein2);
				
				context.write(protein, protein2);
			}

		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
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

	public static class Reduce extends Reducer<ProteinWritable, ProteinWritable, ProteinWritable, ProteinWritable> {

		private String source;
		private String destination;
		private Iterator<ProteinWritable> i;
		private MultipleOutputs mos;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			
			Configuration conf = context.getConfiguration();
			source = conf.get(ProblemIII.SOURCE_PROTEIN_NAME);
			destination = conf.get(ProblemIII.DESTINATION_PROTEIN_NAME);
			
			mos = new MultipleOutputs(context);
			
		}
		
		@Override
		public void reduce(ProteinWritable key, Iterable<ProteinWritable> values, Context context) throws IOException, InterruptedException{
			
			try {
				
				i=values.iterator();
				ProteinWritable pw1 = i.next().clone();
				
				System.out.println("id="+key.id.get()+"| key="+key.name.toString()+"| PW1.name="+pw1.name.toString()+" PW1.id="+pw1.id.get()+"|");
				
				if (pw1.name.toString().equals(source)){
					
					System.out.println("MATCHED WITH SOURCE!");
					
					while(i.hasNext()){
						
						System.out.println("HAD CHILDREN!");
						
						ProteinWritable pw2 = i.next().clone();
						
						pw2.appendToPath(pw1.id.get());
						
						pw2.setName(new Text(FROM_GRAPH));
						
						//context.write(pw2,pw2);
						mos.write("proteins", pw2, pw2);
						
					}
				}

			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
	}
	
	public static class IterableMap extends Mapper<ProteinWritable, ProteinWritable, ProteinWritable, ProteinWritable> {
		
		private boolean isEdgesFile;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().toString();
			
			isEdgesFile = false;
			if(filename.contains("edges")){
				isEdgesFile = true;
			}
			
		}
		
		public void map(ProteinWritable key, ProteinWritable value, Context context) throws IOException, InterruptedException{
			
			if(isEdgesFile){
				key.setName(new Text(FROM_EDGES));
			}
			// We are operating on the results from the last time!
			context.write(key, value);

		}
		
	}
	
	public static class IterableReduce extends Reducer<ProteinWritable, ProteinWritable, ProteinWritable, ProteinWritable> {

		private int destinationNumber;
		private Iterator<ProteinWritable> i;
		private Counter convergenceCounter, destinationFoundCounter;
		private MultipleOutputs<ProteinWritable,ProteinWritable> mos;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			
			Configuration conf = context.getConfiguration();
			destinationNumber = Integer.valueOf(conf.get(ProblemIII.DESTINATION_PROTEIN_NUMBER));
			
			convergenceCounter = context.getCounter(ProblemIII.MyCounter.CONVERGENCE_COUNTER);
			destinationFoundCounter = context.getCounter(ProblemIII.MyCounter.DESTINATION_FOUND_COUNTER);
			
			convergenceCounter.setValue(0);
			destinationFoundCounter.setValue(0);
			
			mos = new MultipleOutputs<ProteinWritable,ProteinWritable>(context);
			
		}
		
		@Override
		public void reduce(ProteinWritable key, Iterable<ProteinWritable> values, Context context) throws IOException, InterruptedException{
			
			try {
				
				i=values.iterator();
				ProteinWritable pw1 = i.next().clone();
				
				if(pw1.name.toString().equals(FROM_GRAPH)){
					
					ProteinWritable pw2;
					
					while(i.hasNext()){
						
						pw2 = i.next().clone();
						
						System.out.println(pw2.path.toString()+" contains "+ String.valueOf(pw1.id.get())+" = "+(pw2.path.toString().indexOf(String.valueOf(pw1.id.get()))));
						
						if(pw2.id.get() != pw1.id.get() && !pw2.path.toString().contains(String.valueOf(pw1.id.get()))){
							
							convergenceCounter.increment(1);
							
							pw2.setPath(pw1.path.toString());
							pw2.appendToPath(pw1.id.get());
							
							System.out.println("For node "+pw2.id.get()+"-> added "+pw1.id.get()+" to path("+pw2.path.toString()+")");
							
							if(pw2.id.get() == destinationNumber && destinationFoundCounter.getValue() == 0){
								String[] path = pw2.path.toString().split(",");
								System.out.println("Found the path!="+pw2.path.toString());
								for(int i=0;i<path.length;i++){
									ProteinWritable pw =  new ProteinWritable(Integer.valueOf(path[i]),"");
									mos.write("result", pw, pw);
								}
								destinationFoundCounter.setValue(1);
							}
							
							pw2.setName(new Text(FROM_GRAPH));
							
							mos.write("proteins", pw2, pw2);
							//context.write(pw2,pw2);
							
						}						
					}
				}

			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
	}
	
	
	public static class FinalReduce extends Reducer<ProteinWritable, ProteinWritable, ProteinWritable, NullWritable> {
		
		private LinkedList<String> path = new LinkedList<String>();
		private Iterator<ProteinWritable> i;
		private String source;
		private String destination;

		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			
			Configuration conf = context.getConfiguration();
			source = conf.get(ProblemIII.SOURCE_PROTEIN_NAME);
			destination = conf.get(ProblemIII.DESTINATION_PROTEIN_NAME);
		
		}
		
		@Override
		public void reduce(ProteinWritable key, Iterable<ProteinWritable> values, Context context) throws IOException, InterruptedException{
		
			i=values.iterator();
			// Advance over the first item
			ProteinWritable pw = i.next();
			
			if (i.hasNext()){
				path.add(pw.name.toString());
			}
		
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			String buffer = source+" ";
			if(path.size()==0){
				buffer+="null ";
			}else{
				for(String protein : path){
					buffer+=protein+" ";
				}
			}
			buffer += destination;
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream out = fs.create(new Path(context.getConfiguration().get(ProblemIII.OUTPUT_FILE)));
			
			out.write(buffer.getBytes());
			
			out.close();
			
		}
		
	}

	public static void main(String[] args) throws Exception {

		GenericOptionsParser parser = new GenericOptionsParser(args);
		Configuration conf = parser.getConfiguration();

		String[] arguments = parser.getRemainingArgs();
		if (arguments.length < 5) {
			usage(System.out);
			System.exit(-1);
		}
		
		FileSystem fs = FileSystem.get(conf);

		conf.set(NODES_FILE, arguments[0]);
		conf.set(EDGES_FILE, arguments[1]);
		
		conf.set(SOURCE_PROTEIN_NAME, arguments[3]);
		conf.set(DESTINATION_PROTEIN_NAME, arguments[4]);
		
		Job job = new Job(conf,"Resolve protein name to node number");
		job.setJarByClass(ProblemIII.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setGroupingComparatorClass(ProteinComparator.class);
		
		job.setMapOutputKeyClass(ProteinWritable.class);
		job.setMapOutputValueClass(ProteinWritable.class);
		
		job.setOutputKeyClass(ProteinWritable.class);
		job.setOutputValueClass(ProteinWritable.class);

		MultipleOutputs.addNamedOutput(job, "source", SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "destination", SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
		
		MultipleOutputs.addNamedOutput(job, "nodes", SequenceFileOutputFormat.class, ProteinWritable.class, ProteinWritable.class);
		MultipleOutputs.addNamedOutput(job, "edges", SequenceFileOutputFormat.class, ProteinWritable.class, ProteinWritable.class);
		MultipleOutputs.addNamedOutput(job, "proteins", SequenceFileOutputFormat.class, ProteinWritable.class, ProteinWritable.class);
		
		MultipleOutputs.addNamedOutput(job, "result", TextOutputFormat.class, Text.class, NullWritable.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		Path tmpPath = new Path("tmp0");
		if(fs.exists(tmpPath)){
	    	fs.delete(tmpPath,true);
	    }
		fs.deleteOnExit(tmpPath);
		
		FileInputFormat.addInputPath(job, new Path(arguments[0]));
		FileInputFormat.addInputPath(job, new Path(arguments[1]));
		
		FileOutputFormat.setOutputPath(job, tmpPath);

		job.waitForCompletion(true);
		
		Counter counter = job.getCounters().findCounter(ProblemIII.MyCounter.ITERATION_COUNT);
		
		int sourceNumber = 0;
		int destinationNumber = 0;
		
		Path path;
		FileStatus[] fss = fs.listStatus(tmpPath);
	    for (FileStatus status : fss) {
	    	// There might be multiple sources if there are multiple reducers, so we accumulate totals
	    	if(status.getPath().getName().contains("source")){
		        path = status.getPath();
		        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		        IntWritable key = new IntWritable();
		        while (reader.next(key)) {
		        	sourceNumber = key.get();
		        }
		        reader.close();
	    	}
	    	if(status.getPath().getName().contains("destination")){
		        path = status.getPath();
		        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		        IntWritable key = new IntWritable();
		        while (reader.next(key)) {
		        	destinationNumber = key.get();
		        }
		        reader.close();
	    	}
	    }
		
		System.out.println("Source Protein found with id="+sourceNumber);
		System.out.println("Destination Protein found with id="+destinationNumber);
		
		// Now the magic starts, we iterate until destination is found
		
		Counter convergenceCounter = null;
		Counter destinationFoundCounter = null;
		
		boolean proteinsFound = false;
		
		do{
			
			Job iterJob = new Job(conf,"Find destination protein (Iterate)");
			iterJob.setJarByClass(ProblemIII.class);
			
			iterJob.getConfiguration().setInt(ProblemIII.SOURCE_PROTEIN_NUMBER, sourceNumber);
			iterJob.getConfiguration().setInt(ProblemIII.DESTINATION_PROTEIN_NUMBER, destinationNumber);

			
			// Add protein files from the previous iteration as input
			proteinsFound = false;
			Path proteinsPath = new Path("tmp"+counter.getValue());
			fss = fs.listStatus(proteinsPath);
			for (FileStatus status : fss) {
				if(status.getPath().getName().contains("proteins")){
					proteinsFound = true;
					System.out.println("Added path \""+status.getPath().getName()+"\"");
					FileInputFormat.addInputPath(iterJob, status.getPath());
				}
			}
			
			if(!proteinsFound){
				System.out.println("No proteins found after expanding from node id="+sourceNumber);
				break;
			}
	
			iterJob.setMapperClass(IterableMap.class);
			iterJob.setReducerClass(IterableReduce.class);
	        
			iterJob.setInputFormatClass(SequenceFileInputFormat.class);
			
			iterJob.setGroupingComparatorClass(ProteinComparator.class);
			
			iterJob.setMapOutputKeyClass(ProteinWritable.class);
			iterJob.setMapOutputValueClass(ProteinWritable.class);
			
			iterJob.setOutputKeyClass(ProteinWritable.class);
			iterJob.setOutputValueClass(ProteinWritable.class);
			
			FileInputFormat.addInputPath(iterJob, new Path("tmp0/edge*"));
			
			MultipleOutputs.addNamedOutput(iterJob, "proteins", SequenceFileOutputFormat.class, ProteinWritable.class, ProteinWritable.class);
			MultipleOutputs.addNamedOutput(iterJob, "result", SequenceFileOutputFormat.class, ProteinWritable.class, ProteinWritable.class);
			
			counter.increment(1);
			
			Path iterPath = new Path("tmp"+counter.getValue());
			if(fs.exists(iterPath)){
				fs.delete(iterPath,true);
			}
			fs.deleteOnExit(iterPath);

			FileOutputFormat.setOutputPath(iterJob, iterPath);
			
			iterJob.waitForCompletion(true);
			
			convergenceCounter = iterJob.getCounters().findCounter(ProblemIII.MyCounter.CONVERGENCE_COUNTER);
			destinationFoundCounter = iterJob.getCounters().findCounter(ProblemIII.MyCounter.DESTINATION_FOUND_COUNTER);
			
		}while(convergenceCounter.getValue() > 0 && destinationFoundCounter.getValue() == 0);
		
		if(proteinsFound){
			System.out.printf("The search concluded. convergenceCounter=%d destinationFoundCounter=%d\n",convergenceCounter.getValue(), destinationFoundCounter.getValue());
		}
		
		Job job2 = new Job(conf,"Resolve path to protein names");
		job2.setJarByClass(ProblemIII.class); 
		
		job2.setReducerClass(FinalReduce.class);
        
		job2.setGroupingComparatorClass(ProteinComparator.class);
		
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		
		job2.setMapOutputKeyClass(ProteinWritable.class);
		job2.setMapOutputValueClass(ProteinWritable.class);
		
		job2.setOutputKeyClass(ProteinWritable.class);
		job2.setOutputValueClass(ProteinWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path("tmp0/nodes*"));
		
		if(proteinsFound){
			Path resultsPath = new Path("tmp"+counter.getValue()+"/result*");
			FileInputFormat.addInputPath(job2, resultsPath);
		}
		
		Path outputPath = new Path(arguments[2]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		
		job2.getConfiguration().set(ProblemIII.OUTPUT_FILE, arguments[2]);
		
		Path lastPath = new Path("final");
		if(fs.exists(lastPath)){
	    	fs.delete(lastPath,true);
	    }
		fs.deleteOnExit(lastPath);
		FileOutputFormat.setOutputPath(job2, lastPath);
		
		job2.waitForCompletion(true);
		
		// Manually delete the temporary directories
		Path pathDelete;
		for(int i=0;i<counter.getValue();i++){
			pathDelete = new Path("tmp"+i);
			if(fs.exists(pathDelete)){
				fs.delete(pathDelete,true);
			}
		}
		
		fs.close();
		
	}

	private static void usage(PrintStream out) {
		out.println("Usage: <path to nodes.txt> <path to edges.txt> <path to output.txt> <source protein name> <destination protein name>");
	}
}
