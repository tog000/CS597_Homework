package edu.boisestate.cs597;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
	public static final String RATINGS_FROM_TARGET_USER = "RATINGS_FROM_TARGET_USER";
	public static final String RATINGS = "RATINGS";
	public static final String SIMILARITIES = "SIMILARITIES";
	
	
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
		//private ArrayList<String> usersWhoNeedRecommendation = new ArrayList<String>();
		private ArrayList<String> moviesNeedingRecommendation = new ArrayList<String>();
		private HashMap<String,LinkedList<String>> moviesNeedingRecommendationByUser = new HashMap<String,LinkedList<String>>();
		
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
				
				if(!moviesNeedingRecommendationByUser.containsKey(parts[0])){
					moviesNeedingRecommendationByUser.put(parts[0], new LinkedList<String>());
				}
				
				moviesNeedingRecommendationByUser.get(parts[0]).add(parts[1]);
				
				//usersWhoNeedRecommendation.add(parts[0]);
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
				if(moviesNeedingRecommendationByUser.containsKey(mrs.user.toString())){
					//int index = usersWhoNeedRecommendation.indexOf(mrs.user.toString());
					//System.out.println("Found a movie that an user who needs recommendation has seen before! "+mrs.user.toString()+"->"+mrs.movie.toString());
					
					LinkedList<String> list = moviesNeedingRecommendationByUser.get(mrs.user.toString());
					
					for(String movie : list){
						//mos.write(mrs.user, mrs.movie, MOVIES_SEEN_BY_USER);//+"_"+mrs.user);
						mos.write(new Text(movie), new Text(mrs.toString()), MOVIES_SEEN_BY_USER);//+"_"+mrs.movie);
					}
					
					mos.write(mrs.movie, new Text(mrs.rating.toString()), RATINGS_FROM_TARGET_USER+"_"+mrs.user.toString());//+"_"+mrs.movie);
					
				}
				
				if(moviesNeedingRecommendation.indexOf(mrs.movie.toString()) >= 0 ){
					//System.out.println("Found a user who saw a the movie we are trying to rate");
					//mos.write(key, new Text(mrs.toString()), USERS_WHO_SAW_MOVIE);//+"_"+mrs.movie);
					mos.write(mrs.user, mrs.movie, USERS_WHO_SAW_MOVIE);//+"_"+mrs.movie);
				}
				

				total += 1;
				sum += mrs.rating.get();
			}
			
			mos.write(key, new Text(String.valueOf(sum/total)), RATINGS);			
			
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
							
							if(targetMovies.get(key.toString()).indexOf(current.user.toString()) < 0){
								targetMovies.get(key.toString()).add(current.user.toString());
							}
							
							current.movie = new Text(key.toString());
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
			
			
			/**
			System.out.println("targetMovies");
			for(Entry<String,LinkedList<String>> entry : targetMovies.entrySet()){
				System.out.println("["+entry.getKey()+"]->"+Arrays.toString(entry.getValue().toArray()));
			}
			
			System.out.println("MoviesSeenByTargetUser");
			for(Entry<String,LinkedList<MovieRatingSimilarity>> entry : moviesSeenByTargetUser.entrySet()){
				System.out.println("["+entry.getKey()+"]->"+Arrays.toString(entry.getValue().toArray()));
			}
			
			System.out.println("usersWhoSawTargetMovie");
			for(Entry<String,LinkedList<String>> entry : usersWhoSawTargetMovie.entrySet()){
				System.out.println("["+entry.getKey()+"]->"+Arrays.toString(entry.getValue().toArray()));
			}
			/**/
			
			
		}
		
		@Override
		public void map(LongWritable byteOffset, Text line, Context context) throws IOException, InterruptedException{
			
			if(byteOffset.get() == 0){
				currentMovie = line.toString().replace(":", "");
				return;
			}

			String parts[] = line.toString().split(",");
			
			if(!isInputFile){
				
				String currentUser = parts[0];
				
				//System.out.println("I'm Movie "+currentMovie);
				
				if (moviesSeenByTargetUser.containsKey(currentMovie)){
					
					//System.out.println("I was seen by target user.");
					
					// We are in a movie that was seen by a target user
					if (usersWhoSawTargetMovie.containsKey(currentUser)){
						
//						for(Entry<String,LinkedList<MovieRatingSimilarity>> entry : moviesSeenByTargetUser.entrySet()){
//							for(MovieRatingSimilarity mrs : entry.getValue()){
//								System.out.println("OLD->"+mrs.user.toString()+"_"+entry.getKey().toString()+" = Rating from "+parts[0]);
//								context.write(new Text(mrs.user.toString()+"_"+entry.getKey().toString()), new MovieRatingSimilarity(currentMovie,parts[0],parts[1]));
//							}
//						}
						
						LinkedList<MovieRatingSimilarity> list = moviesSeenByTargetUser.get(currentMovie);
						for(MovieRatingSimilarity mrs : list){
							//System.out.println("OLD->"+mrs.user.toString()+"_"+mrs.movie.toString()+" = Rating from "+parts[0]);
							context.write(new Text(mrs.user.toString()+"_"+mrs.movie.toString()), new MovieRatingSimilarity(currentMovie,parts[0],parts[1]));
						}
						
						
					}

				
				}
				// We are in a movie we needed to calculate similarity
				if(moviesNeedingRecommendation.indexOf(currentMovie) >= 0){
					
					// We are target movie
					LinkedList<String> list = targetMovies.get(currentMovie);
					
					for(String user : list){
						//context.write(new Text(user+"_"+currentMovie), new MovieRatingSimilarity(currentMovie, parts[0], parts[1]));
						//System.out.println("DIRECT->"+user+"_"+currentMovie+" = Rating from "+parts[0]);
						context.write(new Text(user+"_"+currentMovie), new MovieRatingSimilarity(currentMovie,parts[0],parts[1]));
					}
				}
			}
		}
	}

	
	public static class SimilarityReducer extends Reducer<Text, MovieRatingSimilarity, DoubleWritable, Text>{
		
		private MultipleOutputs<DoubleWritable, Text> mos;
		
		// This is justified by saying that the user ranking can be stored on 6 bytes
		// and there are only 480189 users, which is less than 5MB, therefore we store 
		// it in memory.
		private HashMap<String,Float> userRatings = new HashMap<String,Float>();
		
		@Override
		public void setup(Context context) throws IOException{
			
			mos = new MultipleOutputs(context);
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			
			SequenceFile.Reader reader = null;
			Writable key;
			Writable value;
			
			FileStatus[] fss = fs.listStatus(new Path(conf.get("stage1OutputPath")));
		    for (FileStatus file : fss) {
				try{
					/**/
					if (file.getPath().getName().contains(RATINGS)) {
						reader = new SequenceFile.Reader(fs, file.getPath(), conf);
						key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
						value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
						//long position = reader.getPosition();
						while (reader.next(key, value)) {							
							userRatings.put(key.toString(),Float.valueOf(value.toString()));
						}
					}
					/**/
			    } finally {
			      IOUtils.closeStream(reader);
			    }
		    }
			
		}
		
		@Override
		public void reduce(Text key, Iterable<MovieRatingSimilarity> values, Context context) throws IOException, InterruptedException{
			
			// this is UGLY! But the deadline is looming...
			HashMap<String,HashMap<String, Float>> moviePairUser = new HashMap<String,HashMap<String, Float>>();
			
			for(MovieRatingSimilarity mrs : values){
				//System.out.println("["+key+"] -> "+mrs);
				
				
				if(!moviePairUser.containsKey(mrs.movie.toString())){
					moviePairUser.put(mrs.movie.toString(),new HashMap<String, Float>());
				}
				
				/*
				if(!moviePairUser.get(mrs.movie.toString()).containsKey(mrs.user.toString())){
					//moviePairUser.put(key.toString(),new HashMap<String, LinkedList<Float>>());
					moviePairUser.get(mrs.movie.toString()).put(mrs.user.toString(),new LinkedList<Float>());
				}
				*/
				
				moviePairUser.get(mrs.movie.toString()).put(mrs.user.toString(),Float.valueOf(mrs.rating.get()));
				
			}
			

			/**
			for(Entry<String,HashMap<String, Float>> entry: moviePairUser.entrySet()){
				
				System.out.println("["+entry.getKey()+"]");
				for(Entry<String, Float> entry2 : entry.getValue().entrySet()){
					System.out.println("\t{"+entry2.getKey()+"}"+entry2.getValue());
				}
			}
			/**/
			
			String targetMovie = key.toString().split("_")[1];
			HashMap<String, Float> targetMovieHashMap = moviePairUser.get(targetMovie);
			
			float top = 0;
			float bottom = 0;
			
			for(Entry<String,HashMap<String, Float>> entry: moviePairUser.entrySet()){
				// For every movie in the map except the target movie
				if(!entry.getKey().equals(targetMovie)){
					// For every user in there
					
					Double topPart = 0d;
					Double bottomPart1 = 0d;
					Double bottomPart2 = 0d;
					
					for(Entry<String, Float> entry2 : entry.getValue().entrySet()){
						
						if(targetMovieHashMap.containsKey(entry2.getKey())){
							Float Rau = targetMovieHashMap.get(entry2.getKey());
							Float Rbu = entry2.getValue();
							
							Float Ravgu = userRatings.get(entry2.getKey());
							
							topPart += (Rau-Ravgu)*(Rbu-Ravgu);
							bottomPart1 += Math.pow((double)(Rau-Ravgu),2d);
							bottomPart2 += Math.pow((double)(Rbu-Ravgu),2d);
							
						}
						
						
					}
					
					Double similarity = topPart/(Math.sqrt(bottomPart1*bottomPart2)); 
					
					mos.write(new DoubleWritable(similarity), new Text(entry.getKey()), SIMILARITIES+"_"+key.toString());//+"_"+mrs.movie);
					
				}
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
        //LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        
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
		LazyOutputFormat.setOutputFormatClass(job2, SequenceFileOutputFormat.class);
		
		job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SimilarityReducer.class);
        
		job2.setJobName("Stage II");
		
        Path outputPath = new Path(options[1]);
		if(fs.exists(outputPath)){
	    	fs.delete(outputPath,true);
	    }
		
		FileInputFormat.addInputPath(job2, new Path(options[0]));
		FileOutputFormat.setOutputPath(job2, outputPath);

		job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MovieRatingSimilarity.class);
        
        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(Text.class);
		
		job2.getConfiguration().set("inputFile", options[2]);
		job2.getConfiguration().set("stage1OutputPath", outputPathStage1.toString());
		
		job2.waitForCompletion(true);
		
		//ArrayList<String> moviesNeedingRecommendation = new ArrayList<String>();
		//HashMap<String,LinkedList<String>> moviesNeedingRecommendationByUser = new HashMap<String,LinkedList<String>>();
		// SequenceFile.Reader reader = new SequenceFile.Reader(fs,fileSplit.getPath(), context.getConfiguration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(options[2]))));
		String parts[];

		FSDataOutputStream out = fs.create(new Path(outputPath.toString()+"/final_output_predictions.txt"));
		
		String line = br.readLine();
		while (line != null) {
			parts = line.split(",");
			
			//System.out.println("Read line: "+line);
			
			String currentUser = parts[0];
			String currentMovie = parts[1];
			
			HashMap<String, Float> ratingsFromUser = new HashMap<String, Float>();
			HashMap<String, Float> similaritiesToTargetMovie = new HashMap<String, Float>();
			
			SequenceFile.Reader reader = null;
			Writable key;
			Writable value;
			FileStatus[] fss = fs.listStatus(outputPathStage1);
		    for (FileStatus file : fss) {
				try{
					//System.out.println("Listed file STAGE1:"+file.getPath());
					/**/
					if (file.getPath().getName().contains(RATINGS_FROM_TARGET_USER+"_"+currentUser+"-")) {
						reader = new SequenceFile.Reader(fs, file.getPath(), conf);
						key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
						value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
						//long position = reader.getPosition();
						while (reader.next(key, value)) {			
							//System.out.println("Added rating from user="+key.toString()+" and "+value.toString());
							ratingsFromUser.put(key.toString(),Float.valueOf(value.toString()));
						}
					}
					/**/
			    } finally {
			      IOUtils.closeStream(reader);
			    }
		    }
			
			fss = fs.listStatus(outputPath);
		    for (FileStatus file : fss) {
				try{
					//System.out.println("Listed file FINAL:"+file.getPath());
					/**/
					if (file.getPath().getName().contains(SIMILARITIES+"_"+currentUser+"_"+currentMovie+"-")) {
						reader = new SequenceFile.Reader(fs, file.getPath(), conf);
						key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
						value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
						//long position = reader.getPosition();
						while (reader.next(key, value)) {
							//System.out.println("Added similarity from movir="+value.toString()+" and "+key.toString());
							similaritiesToTargetMovie.put(value.toString(),Float.valueOf(key.toString()));
						}
					}
					/**/
			    } finally {
			      IOUtils.closeStream(reader);
			    }
		    }
			
		    int movieCounter = 0;
		    double topPart = 0;
		    double bottomPart = 0;
		    
		    for(Entry<String, Float> entry : similaritiesToTargetMovie.entrySet()){
		    	if(movieCounter > 7){
		    		break;
		    	}
		    	
		    	topPart += entry.getValue()*ratingsFromUser.get(entry.getKey());
		    	bottomPart += Math.abs(entry.getValue());
		    	//System.out.println("topPart="+topPart);
		    	//System.out.println("bottomPart="+bottomPart);
		    	
		    	movieCounter++;
		    }
			
		    double result = topPart/bottomPart;
		    
		    out.writeChars(line+","+result+"\n");
		    System.out.printf(line+","+result+"\n");
			
			line = br.readLine();
		}

		br.close();
		
		out.close();
		
		
		
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
