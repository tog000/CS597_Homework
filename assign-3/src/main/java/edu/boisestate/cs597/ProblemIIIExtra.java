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

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;


public class ProblemIIIExtra {

	public static final String OUTPUT_FILE = "ofile";
	
	public static final String NODES_FILE = "nfile";
	public static final String EDGES_FILE = "efile";
	
	public static final String PROTEINS_LIST = "prot";
	public static final String PROTEIN_LIST_NUMBERS = "protn";
	
	public static final String DESTINATION_FOUND = "dstf";
	
	public static final String FROM_EDGES = "BBB";
	public static final String FROM_GRAPH = "AAA";
	
	public enum MyCounter{
		  ITERATION_COUNT,
		  CONVERGENCE_COUNTER,
		  DESTINATION_FOUND_COUNTER
	}	
	
	public static class Map extends Mapper<LongWritable, Text, ProteinWritable, ProteinWritable> {
		
		private String proteinsList;
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
			proteinsList = conf.get(PROTEINS_LIST);
			
			System.out.println("Searching for proteins = "+proteinsList);
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().toString();
			
			isNodesFile = false;
			if(filename.endsWith(conf.get(NODES_FILE))){
				isNodesFile = true;
			}
			// Cant parametrize! Stop complaining about it!
			mos = new MultipleOutputs(context);
			
		}
		
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
			
			currentLine = line.toString();
			
			scn = new Scanner(currentLine);
			
			if(isNodesFile){
				
				protein = new ProteinWritable(scn.nextInt(),scn.next().replace("\"", ""));				
				mos.write("nodes", protein, protein);
				
				context.write(protein, protein);
				
				if(proteinsList.contains(protein.name.toString())){
					mos.write("proteinnumbers", protein.id, NullWritable.get());
					System.out.println("Resolved a protein="+protein.id.get()+" -> "+protein.name);
				}
				
			}else{
				protein = new ProteinWritable(scn.nextInt(),"");
				protein2 = new ProteinWritable(scn.nextInt(),"");
				
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

		private String proteinsList;
		private Iterator<ProteinWritable> i;
		private MultipleOutputs<ProteinWritable,ProteinWritable> mos;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			
			Configuration conf = context.getConfiguration();
			proteinsList = conf.get(PROTEINS_LIST);
			mos = new MultipleOutputs<ProteinWritable,ProteinWritable>(context);
			
		}
		
		@Override
		public void reduce(ProteinWritable key, Iterable<ProteinWritable> values, Context context) throws IOException, InterruptedException{
			
			try {
				
				i=values.iterator();
				ProteinWritable pw1 = i.next().clone();
				
				// If we've found one of the source node
				if (proteinsList.contains(pw1.name.toString())){
					
					// Add the all its edges (MSP of this subgraph) 
					while(i.hasNext()){
						
						ProteinWritable pw2 = i.next().clone();
						
						pw2.setName(FROM_GRAPH);
						
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
				key.setName(FROM_EDGES);
			}else{
				key.setName(FROM_GRAPH);
			}
			// We are operating on the results from the last time!
			context.write(key, value);

		}
		
	}
	
	public static class IterableReduce extends Reducer<ProteinWritable, ProteinWritable, ProteinWritable, ProteinWritable> {

		private String proteinsListNumber;
		private int sourceNumber;
		private Iterator<ProteinWritable> i;
		private Counter convergenceCounter, destinationFoundCounter;
		private MultipleOutputs<ProteinWritable,ProteinWritable> mos;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			
			Configuration conf = context.getConfiguration();
			proteinsListNumber = conf.get(PROTEIN_LIST_NUMBERS);
			
			convergenceCounter = context.getCounter(MyCounter.CONVERGENCE_COUNTER);
			destinationFoundCounter = context.getCounter(MyCounter.DESTINATION_FOUND_COUNTER);
			
			convergenceCounter.setValue(0);
			destinationFoundCounter.setValue(0);
			
			mos = new MultipleOutputs<ProteinWritable,ProteinWritable>(context);
			
		}
		
		@Override
		public void reduce(ProteinWritable key, Iterable<ProteinWritable> values, Context context) throws IOException, InterruptedException{
			
			try {
				
				boolean MSTFound = false;
				
				i=values.iterator();
				ProteinWritable pw1 = i.next().clone();
				ProteinWritable pw2;
				
				// If this node is coming from the proteins file (from the search)
				if(pw1.name.toString().equals(FROM_GRAPH)){
					
					// Check to see if we've found the destination
					MSTFound = checkForMST(pw1);
					
					// For each of its neighbors 
					while(i.hasNext() && !MSTFound){
						
						pw2 = i.next().clone();
						
						// If this is not a recurrent edge AND if we haven't traveled through this node yet
						if(pw2.id.get() != pw1.id.get() && !pw2.path.toString().contains(String.valueOf(pw1.id.get()))){
							
							// Increase the convergence: we are still traversing the graph
							convergenceCounter.increment(1);
							
							// Set the path to the parent path, and add the parent id 
							pw2.setPath(pw1.path.toString());
							pw2.appendToPath(pw1.id.get());
							
							// Check to see if we've found the destination, if found, break
							if(checkForMST(pw2)){
								break;
							}
							
							pw2.setName(FROM_GRAPH);
							
							mos.write("proteins", pw2, pw2);							
						}
					}
				}

			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}
		
		public boolean checkForMST(ProteinWritable protein) throws IOException, InterruptedException{
			
			ProteinWritable rpw;
			
			// If the protein happens to be the destination AND the destination hasn't been found yet
			String proteins[] = proteinsListNumber.split(",");
			String path = protein.path.toString();
			path += protein.id.toString();
			
			System.out.println("Evaluating on node "+ protein.id +" if this path is a MST.\nPath="+path+"   MST="+proteinsListNumber+"\n");
			
			String paths[] = path.split(",");
			boolean MSTComplete = true;
			boolean matches;
			for(int i=0;i<proteins.length;i++){
				matches = false;
				for(int j=0;j<paths.length;j++){
					if(paths[j].equals(proteins[i])){
						matches = true;
						break;
					}
				}
				if(!matches){
					MSTComplete = false;
					break;
				}
			}
			
			
			
			if(MSTComplete && destinationFoundCounter.getValue() == 0){
				
				System.out.println("FOUND THE MST->"+protein.path.toString());
				
				// For every element in the path except the last (its the destination)
				for(int i=0;i<paths.length;i++){
					// Recreate the path
					rpw =  new ProteinWritable(Integer.valueOf(paths[i]),"");
					// Write to the result file
					mos.write("result", rpw, rpw);
				}
				
				// Set the counter to FOUND
				destinationFoundCounter.setValue(1);
				
				return true;
				
			}
			
			return false;
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
		
	}
	
	
	public static class FinalReduce extends Reducer<ProteinWritable, ProteinWritable, ProteinWritable, NullWritable> {
		
		private LinkedList<String> path = new LinkedList<String>();
		private Iterator<ProteinWritable> i;
		private String proteinList;
		private boolean destinationFound;

		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			
			Configuration conf = context.getConfiguration();
			destinationFound = conf.getBoolean(DESTINATION_FOUND,false);
			proteinList = conf.get(PROTEINS_LIST);
		}
		
		@Override
		public void reduce(ProteinWritable key, Iterable<ProteinWritable> values, Context context) throws IOException, InterruptedException{
		
			i=values.iterator();
			// Advance over the first item
			ProteinWritable pw = i.next();
			
			if (i.hasNext()){
				System.out.println("Added \""+pw.name.toString()+"\" node to the list with ID->"+pw.id.toString());
				path.add(pw.name.toString());
			}
		
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			
			// Add the source to the result
			String buffer = "";
			// Add the source to the result
			if(!destinationFound){
				buffer+="MST  "+proteinList+" does not exist";
			}else{
				for(int i=1;i<path.size();i++){
					buffer+=path.get(i-1)+" "+path.get(i)+"\n";
				}
			}
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream out = fs.create(new Path(context.getConfiguration().get(OUTPUT_FILE)));
			
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
		
		String proteinsList = arguments[3];
		for(int i=3;i<arguments.length;i++){
			proteinsList += " "+arguments[i];
		}
		
		conf.set(PROTEINS_LIST, proteinsList);
		
		Job job = new Job(conf,"Resolve protein name to node number");
		job.setJarByClass(ProblemIII.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setGroupingComparatorClass(ProteinComparator.class);
		
		job.setMapOutputKeyClass(ProteinWritable.class);
		job.setMapOutputValueClass(ProteinWritable.class);
		
		job.setOutputKeyClass(ProteinWritable.class);
		job.setOutputValueClass(ProteinWritable.class);

		MultipleOutputs.addNamedOutput(job, "proteinnumbers", SequenceFileOutputFormat.class, IntWritable.class, NullWritable.class);
		
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
		
		Counter counter = job.getCounters().findCounter(MyCounter.ITERATION_COUNT);
		
		String proteinNumbers = "";
		
		Path path;
		FileStatus[] fss = fs.listStatus(tmpPath);
	    for (FileStatus status : fss) {
	    	if(status.getPath().getName().contains("proteinnumbers")){
		        path = status.getPath();
		        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		        IntWritable key = new IntWritable();
		        reader.next(key);
		        proteinNumbers = String.valueOf(key.get());
		        while (reader.next(key)) {
		        	proteinNumbers += ","+key.get();
		        }
		        reader.close();
	    	}
	    }
		
		System.out.println("Proteins in the MST = "+proteinNumbers);
		
		// Now the magic starts, we iterate until destination is found
		
		Counter convergenceCounter = null;
		Counter destinationFoundCounter = null;
		
		boolean proteinsFound = false;
	
		do{
			
			Job iterJob = new Job(conf,"Find MST (Iterate)");
			iterJob.setJarByClass(ProblemIII.class);
			
			iterJob.getConfiguration().set(PROTEIN_LIST_NUMBERS, proteinNumbers);
			
			// Add protein files from the previous iteration as input
			proteinsFound = false;
			Path proteinsPath = new Path("tmp"+counter.getValue());
			fss = fs.listStatus(proteinsPath);
			for (FileStatus status : fss) {
				if(status.getPath().getName().contains("proteins")){
					proteinsFound = true;
					System.out.println("Added path "+proteinsPath.getName()+" \""+status.getPath().getName()+"\"");
					FileInputFormat.addInputPath(iterJob, status.getPath());
				}
			}
			
			System.out.println("Current Paths:");
			
			Path[] paths = FileInputFormat.getInputPaths(iterJob);
			for (Path p : paths){
				System.out.println(p.getParent().getName()+"/"+p.getName());
			}
			
			if(!proteinsFound){
				System.out.println("No more proteins found.");
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
			
			convergenceCounter = iterJob.getCounters().findCounter(MyCounter.CONVERGENCE_COUNTER);
			destinationFoundCounter = iterJob.getCounters().findCounter(MyCounter.DESTINATION_FOUND_COUNTER);
			
		}while(proteinsFound && convergenceCounter.getValue() > 0 && destinationFoundCounter.getValue() == 0 && proteinsFound);
		
		if(proteinsFound){
			System.out.printf("The search concluded. convergenceCounter=%d destinationFoundCounter=%d\n",convergenceCounter.getValue(), destinationFoundCounter.getValue());
		}

		Job job2 = new Job(conf,"Resolve ids to protein names");
		job2.setJarByClass(ProblemIII.class); 
		
		job2.getConfiguration().setBoolean(DESTINATION_FOUND, destinationFoundCounter != null && destinationFoundCounter.getValue() == 1);
		
		job2.setReducerClass(FinalReduce.class);
        
		job2.setGroupingComparatorClass(ProteinComparator.class);
		
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		
		job2.setMapOutputKeyClass(ProteinWritable.class);
		job2.setMapOutputValueClass(ProteinWritable.class);
		
		job2.setOutputKeyClass(ProteinWritable.class);
		job2.setOutputValueClass(ProteinWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path("tmp0/nodes*"));
		
		if(proteinsFound && (convergenceCounter.getValue()>0 || destinationFoundCounter.getValue() == 1)){
			Path resultsPath = new Path("tmp"+counter.getValue()+"/result*");
			FileInputFormat.addInputPath(job2, resultsPath);
		}
		
		Path outputPath = new Path(arguments[2]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		
		job2.getConfiguration().set(OUTPUT_FILE, arguments[2]);
		
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
		out.println("Usage: <path to nodes.txt> <path to edges.txt> <path to output.txt> <protein names>");
	}
}
