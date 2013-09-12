package edu.boisestate.cs597;

/**
 * Matrix Multiplication
 *
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class ProblemII {

	private static final String OUTPUT_PATH = "OutputPath";
	private static final String MATRIX_B = "MatrixB";
	private static final String MATRIX_A = "MatrixA";
	private static final String MATRIX_WIDTH = "MatrixWidth";
	private static final String MATRIX_HEIGHT = "MatrixHeight";

	// @TODO: Change the Key/Value types of the Map to suite your problem
	// definition
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private boolean leftOrRight = false; // Left Matrix or Right Matrix,
												// false = left, true = right.
		private int matrixWidth;
		private int matrixHeight;

		@Override
		protected void setup(Context context) throws IOException,InterruptedException {

			// get filename
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().toString();
			Configuration conf = context.getConfiguration();
			String leftMatrix = conf.get(ProblemII.MATRIX_A);
			String rightMatrix = conf.get(ProblemII.MATRIX_B);
			
			matrixWidth  = conf.getInt(ProblemII.MATRIX_WIDTH,0);
			matrixHeight = conf.getInt(ProblemII.MATRIX_HEIGHT,0);

			if (filename.endsWith(leftMatrix)) {
				leftOrRight = false;
			}
			if (filename.endsWith(rightMatrix)) {
				leftOrRight = true;
			}

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			long lineNumber = key.get() / (value.getLength()+1);
			
			System.out.println("key="+key.toString()+" length="+value.getLength());
			
			// Parse line (tab delimited)
			String[] parts = value.toString().split("\t");
			for(int i=0;i<parts.length;i++){
				if(leftOrRight){
					// If we are the B matrix
					for(int j=0;j<matrixWidth;j++){
						context.write(new Text(String.valueOf(j)+","+String.valueOf(i)+","+(1+lineNumber*2)),new IntWritable(Integer.valueOf(parts[i])));
					}
				}else{
					// If we are the A matrix
					for(int j=0;j<matrixHeight;j++){
						context.write(new Text(String.valueOf(lineNumber)+","+String.valueOf(j)+","+(i*2)),new IntWritable(Integer.valueOf(parts[i])));
					}
				}
			}
			
		}

	}

	public static class PartialKeyComparator extends WritableComparator {
		protected PartialKeyComparator() {
			super(Text.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			String[] t1 = ((Text) w1).toString().split(",");
			String[] t2 = ((Text) w2).toString().split(",");
			
			String t1c = t1[0]+t1[1];
			String t2c = t2[0]+t2[1];
			
			return t1c.compareTo(t2c);
		}
	}

	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		Iterator<IntWritable> i;
		FSDataOutputStream out;

		public void setup(Context context) throws IOException,InterruptedException {
			
			Configuration config = context.getConfiguration();
			
			FileSystem fs = FileSystem.get(config);
			
			// Why doesn't this work?!?!
			//out = fs.create(new Path(config.get(ProblemII.OUTPUT_PATH)));
			
			out = fs.create(new Path("output_matrix"));
			
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			String[] parts = key.toString().split(",");
			
			i = values.iterator();
			int runningTotal = 0;
			while(i.hasNext()){
				
				Integer fromA = i.next().get();
				Integer fromB = i.next().get();
				
				runningTotal+=fromA*fromB;
				
				System.out.println(key.toString()+"-> "+fromA+"*"+fromB+" +="+runningTotal);
				
			}
			
			if(!parts[0].equals("0") && parts[1].equals("0")){
				out.writeBytes("\n");
			}
			
			out.writeBytes(runningTotal+"\t");
			
		}
		
		protected void cleanup(Context context) throws IOException,InterruptedException {
			out.close();
		}

	}

	public static void main(String[] args) throws Exception {
		
		if (args.length != 5) {
			System.err.println("Usage ProblemII <MatrixA> <MatrixB> <MatrixAB> <MatrixA Height> <MatrixB Width>");
			System.exit(-1);
		}

		GenericOptionsParser parser = new GenericOptionsParser(args);
		Configuration conf = parser.getConfiguration();

		String[] arguments = parser.getRemainingArgs();

		conf.set(MATRIX_A, arguments[0]);
		conf.set(MATRIX_B, arguments[1]);
		
		conf.set(MATRIX_WIDTH, arguments[3]);
		conf.set(MATRIX_HEIGHT, arguments[4]);

		Job job = new Job(conf);
		job.setJarByClass(ProblemII.class);
		
		job.setGroupingComparatorClass(PartialKeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(arguments[0]));
		FileInputFormat.addInputPath(job, new Path(arguments[1]));
		
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path("output");
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		
		conf.set(OUTPUT_PATH, arguments[2]);
		
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
