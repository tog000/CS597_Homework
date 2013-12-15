package edu.boisestate.cs597;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

import edu.boisestate.cs587.model.MovieRatingSimilarity;

public class App {

	
	/*
	
	TODO
	
	prediction(movie m, user u){
		
		foreach (7 similar movies to m) as K{
			
			sum1 += similarity(K, m) * Rating of u for K
			sum2 += abs(similarity(K, m));
			
		}
		
		return sum1/sum2;
		
	}
	
	similarity(movie a, movie b){
		
		foreach (user that has ranked movie a and b) as U{
			sum1+=ranking(u,a)-avg(ranking from u) * ranking(u,b)-avg(ranking from u);
			sum2+=ranking(u,a)-avg(ranking from u)^2 * ranking(u,b)-avg(ranking from u)^2;
		}
	
	}
	
	
	User U, Movie B
	
	- Find all movies seen by U as M[]
	- Find users who have seen movies in M[] and also B as OU[] 
	- Calculate average ranking for OU[]
	- Calculate similarity of M[] to B
	- Calculate prediction
	
	
	/**/
	
	public static final String MOVIES_SEEN_BY_USER = "MOVIES_SEEN_BY_USER";
	public static final String USERS_WHO_SAW_MOVIE = "USERS_WHO_SAW_MOVIE";
	
	public static class InitialMapper extends Mapper<LongWritable, Text, Text, MovieRatingSimilarity>{

		private String currentMovie = null;
		private boolean isInputFile=false;
		
		@Override
		public void setup(Context context) throws IOException{
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().toString();
			
			if(filename.contains(context.getConfiguration().get("inputFile"))){
				isInputFile = true;
			}
			
		}
		
		@Override
		public void map(LongWritable byteOffset, Text line, Context context) throws IOException, InterruptedException{
			if(byteOffset.get() == 0){
				currentMovie = line.toString().replace(":", "");
				return;
			}

			String parts[] = line.toString().split(",");
			
			if(!isInputFile){
				context.write(new Text(parts[0]), new MovieRatingSimilarity(currentMovie, parts[0], parts[1]));
			}
		}
	}
	
	public static class InitialReducer extends Reducer<Text, MovieRatingSimilarity, Text, Text>{
		
		private MultipleOutputs<Text, Text> mos;
		private ArrayList<String> usersWhoNeedRecommendation = new ArrayList<String>();
		private ArrayList<String> moviesNeedingRecommendation = new ArrayList<String>();
		
		@Override
		public void setup(Context context) throws IOException{
			
			mos = new MultipleOutputs(context);
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			
			// SequenceFile.Reader reader = new SequenceFile.Reader(fs,fileSplit.getPath(), context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(context.getConfiguration().get("inputFile")))));
			String parts[];

			String line = br.readLine();
			while (line != null) {
				parts = line.split(",");
				usersWhoNeedRecommendation.add(parts[0]);
				moviesNeedingRecommendation.add(parts[1]);
				line = br.readLine();
			}

			br.close();
			
			//System.out.println("usersWhoNeedRecommendation="+Arrays.toString(usersWhoNeedRecommendation.toArray()));
			//System.out.println("moviesNeedingRecommendation"+Arrays.toString(moviesNeedingRecommendation.toArray()));
			
		}
		
		@Override
		public void reduce(Text key, Iterable<MovieRatingSimilarity> values, Context context) throws IOException, InterruptedException{
			
			float sum = 0;
			float total = 0;
			
			for(MovieRatingSimilarity mrs : values){
				
				// If the user who needs a recommendation rated this current movie
				if(usersWhoNeedRecommendation.indexOf(mrs.user.toString()) >= 0 ){
					//System.out.println("Found a movie that an user who needs recommendation has seen before");
					//mos.write(mrs.user, mrs.movie, MOVIES_SEEN_BY_USER);//+"_"+mrs.user);
					int index = usersWhoNeedRecommendation.indexOf(mrs.user.toString());
					mos.write(new Text(moviesNeedingRecommendation.get(index)), new Text(mrs.toString()), MOVIES_SEEN_BY_USER);//+"_"+mrs.movie);
					
				}
				
				if(moviesNeedingRecommendation.indexOf(mrs.movie.toString()) >= 0 ){
					//System.out.println("Found a user who saw a the movie we are trying to rate");
					//mos.write(key, new Text(mrs.toString()), USERS_WHO_SAW_MOVIE);//+"_"+mrs.movie);
					mos.write(mrs.user, mrs.movie, USERS_WHO_SAW_MOVIE);//+"_"+mrs.movie);
				}

				total += 1;
				sum += mrs.rating.get();
			}
			
			mos.write(key, new Text(String.valueOf(sum/total)), "RATINGS");			
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	public static class SecondMapper extends Mapper<LongWritable, Text, Text, MovieRatingSimilarity>{

		private String currentMovie = null;
		private boolean isInputFile=false;
		
		
		private HashMap<String,LinkedList<String>> targetMovies = new HashMap<String,LinkedList<String>>();
		private HashMap<String,LinkedList<MovieRatingSimilarity>> moviesSeenByTargetUser = new HashMap<String,LinkedList<MovieRatingSimilarity>>();
		private HashMap<String,LinkedList<String>> usersWhoSawTargetMovie = new HashMap<String,LinkedList<String>>();
		private ArrayList<String> moviesNeedingRecommendation = new ArrayList<String>();
		
		// This is justified by saying that the user ranking can be stored on 6 bytes
		// and there are only 480189 users, which is less than 5MB, therefore we store 
		// it in memory.
		private HashMap<String,String> userRatings = new HashMap<String,String>();
		
		@Override
		public void setup(Context context) throws IOException{
			
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().toString();
			
			if(filename.contains(context.getConfiguration().get("inputFile"))){
				isInputFile = true;
			}
			
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			
			SequenceFile.Reader reader = null;
			Writable key;
			Writable value;
			
			FileStatus[] fss = fs.listStatus(new Path(conf.get("stage1OutputPath")));
		    for (FileStatus file : fss) {

				try {

					if (file.getPath().getName().contains(MOVIES_SEEN_BY_USER)) {
						reader = new SequenceFile.Reader(fs, file.getPath(), conf);
						key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
						value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
						// long position = reader.getPosition();
						while (reader.next(key, value)) {
							
							MovieRatingSimilarity current = MovieRatingSimilarity.parse(value.toString());
							currentMovie = current.movie.toString();
							
							if(!moviesSeenByTargetUser.containsKey(currentMovie)){
								moviesSeenByTargetUser.put(currentMovie, new LinkedList<MovieRatingSimilarity>());
							}
							
							if(!targetMovies.containsKey(key.toString())){
								targetMovies.put(key.toString(), new LinkedList<String>());
							}
							
							targetMovies.get(key.toString()).add(current.user.toString());
							
							current.movie = (Text)key;
							moviesSeenByTargetUser.get(currentMovie).add(current);
						}
					}
					if (file.getPath().getName().contains(USERS_WHO_SAW_MOVIE)) {
						reader = new SequenceFile.Reader(fs, file.getPath(), conf);
						key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
						value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
						//long position = reader.getPosition();
						while (reader.next(key, value)) {
							
							if(!usersWhoSawTargetMovie.containsKey(key.toString())){
								usersWhoSawTargetMovie.put(key.toString(), new LinkedList<String>());
							}
							
							usersWhoSawTargetMovie.get(key.toString()).add(value.toString());
						}
					}
					/**
					if (file.getPath().getName().contains("RATINGS")) {
						reader = new SequenceFile.Reader(fs, file.getPath(), conf);
						key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
						value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
						//long position = reader.getPosition();
						while (reader.next(key, value)) {
							userRatings.put(key.toString(),value.toString());
						}
					}
					/**/
			    } finally {
			      IOUtils.closeStream(reader);
			    }
		    }
		    
		    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(context.getConfiguration().get("inputFile")))));
			String parts[];

			String line = br.readLine();
			while (line != null) {
				parts = line.split(",");
				moviesNeedingRecommendation.add(parts[1]);
				line = br.readLine();
			}

			br.close();
			
		}
		
		@Override
		public void map(LongWritable byteOffset, Text line, Context context) throws IOException, InterruptedException{
			
			if(byteOffset.get() == 0){
				currentMovie = line.toString().replace(":", "");
				return;
			}

			String parts[] = line.toString().split(",");
			
			if(!isInputFile){
				
				String currentUser = parts[1];
				
				if (moviesSeenByTargetUser.containsKey(currentMovie)){
					
					System.out.println("Has user "+currentUser+" seen movie "+currentMovie);
					// We are in a movie we needed to calculate similarity
					if (usersWhoSawTargetMovie.containsKey(currentUser)){
						// This user has seen target movie
						System.out.println("YES");
						// Find the target movies number 
						LinkedList<MovieRatingSimilarity> list = moviesSeenByTargetUser.get(currentMovie);
						
						// We are already seen movies
						for(MovieRatingSimilarity mrs : list){

							// This user was required to rank currentMovie because he saw targetMovie
							if(mrs.movie.toString().equals(usersWhoSawTargetMovie.get(currentUser))){
								context.write(new Text(mrs.user.toString()+"_"+mrs.movie.toString()), new MovieRatingSimilarity(currentMovie, parts[0], parts[1]));
								System.out.println(mrs.user.toString()+"_"+mrs.movie.toString()+" = Rating from "+parts[0]);
							}
						}
					}else if(moviesNeedingRecommendation.indexOf(currentMovie) >= 0){
						
						// We are target movie
						
						LinkedList<String> list = targetMovies.get(currentMovie);
						
						// We are already seen movies
						for(String user : list){
							context.write(new Text(user+"_"+currentMovie), new MovieRatingSimilarity(currentMovie, parts[0], parts[1]));
							System.out.println(user+"_"+currentMovie+" = Rating from "+parts[0]);
						}
						
					}
				}
			}
		}
	}
	
	/**
	public static class SimilarityMapper extends Mapper<LongWritable, Text, Text, MovieRatingSimilarity>{
		@Override
		public void setup(Context context){
			
			this.userID = context.getConfiguration().get("userID");
			
		}
		
		private String currentMovie = null;
		private String userID;
		
		
		@Override
		public void map(LongWritable byteOffset, Text line, Context context) throws IOException, InterruptedException{
			if(byteOffset.get() == 0){
				currentMovie = line.toString().replace(":", "");
				return;
			}

			String parts[] = line.toString().split(",");
			
			//System.out.printf("Just read=%s. User=%s, UserID=%s\n",line,parts[0],userID);
			
			if(parts[0].equals(userID)){
				context.write(new Text(KEY_MOVIES_FROM_USER), new MovieRatingSimilarity(currentMovie, parts[0], parts[1]));
			}
			
			context.write(new Text(parts[0]), new MovieRatingSimilarity(currentMovie, parts[0], parts[1]));
		}
	}
	
	public static class SimilarityReducer extends Reducer<Text, MovieRatingSimilarity, Text, FloatWritable>{
		
		private MultipleOutputs<Text, Text> mos;
		
		@Override
		public void setup(Context context){
			mos = new MultipleOutputs(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<MovieRatingSimilarity> values, Context context) throws IOException, InterruptedException{
			
			float sum = 0;
			float total = 0;
			
			for(MovieRatingSimilarity mrs : values){
				if(key.toString().equals(KEY_MOVIES_FROM_USER)){
					System.out.println("MOVIES FROM USER!");
					mos.write(mrs.movie, new Text(mrs.toString()), "movies_from_user");
				}
				total += 1;
				sum += mrs.rating.get();				
			}
			
			if(!key.toString().equals(KEY_MOVIES_FROM_USER)){
				//context.write(key,new FloatWritable(sum/total));
				mos.write(key, new Text(String.valueOf(sum/total)), "rating_averages");
			}
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	/**/
	
	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
		
		GenericOptionsParser gop = new GenericOptionsParser(args);
		String[] options = gop.getRemainingArgs();
		

        /*
         * STAGE I
         */
		
		Job job1 = new Job(new Configuration());
		job1.setJarByClass(App.class);
		
		// Binary FTW
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job1.setMapperClass(InitialMapper.class);
        job1.setReducerClass(InitialReducer.class);
        
        job1.setJobName("Stage I");
        
        LazyOutputFormat.setOutputFormatClass(job1, SequenceFileOutputFormat.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(MovieRatingSimilarity.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        // Input
        FileInputFormat.addInputPath(job1, new Path(options[0]));

        Path outputPathStage1 = new Path("first_step");
		if(fs.exists(outputPathStage1)){
	    	fs.delete(outputPathStage1,true);
	    }
		//fs.deleteOnExit(outputPath);
		
        FileOutputFormat.setOutputPath(job1, outputPathStage1);
        
        job1.getConfiguration().set("inputFile", options[2]);
        
        job1.waitForCompletion(true);
        
        System.out.println("Sarting stage 2");
        
        
        /*
         * STAGE II
         */
        
        
        Job job2 = new Job(new Configuration());
		job2.setJarByClass(App.class);
		
		// Binary FTW
		//correlationJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job2.setMapperClass(SecondMapper.class);
        //job2.setReducerClass(InitialReducer.class);
        
		job2.setJobName("Stage II");
		
        Path outputPath = new Path(options[1]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		
		FileInputFormat.addInputPath(job2, new Path(options[0]));
		FileOutputFormat.setOutputPath(job2, outputPath);
		
		job2.getConfiguration().set("inputFile", options[2]);
		job2.getConfiguration().set("stage1OutputPath", outputPathStage1.toString());
		
		job2.waitForCompletion(true);
		
        //System.exit(job1.waitForCompletion(true) ? 1 : 0);
        /**
        Path path;
		FileStatus[] fss = fs.listStatus(tmpPath);
	    for (FileStatus status : fss) {
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
        
        /**/
	}
	
}
