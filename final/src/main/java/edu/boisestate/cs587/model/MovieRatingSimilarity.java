package edu.boisestate.cs587.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MovieRatingSimilarity implements WritableComparable<MovieRatingSimilarity>, Cloneable {
	public Text movie;
	public Text user;
	public FloatWritable rating;
	public FloatWritable similarity;

	public MovieRatingSimilarity() {
		movie = new Text();
		user = new Text();
		rating = new FloatWritable();
		similarity = new FloatWritable();
	}
	
	public MovieRatingSimilarity(String movie, String user, String rating) {
		this.movie = new Text(movie);
		this.user = new Text(user);
		this.rating = new FloatWritable(Float.valueOf(rating));
		this.similarity = new FloatWritable(0.0f);
	}
	
	public MovieRatingSimilarity(String movie, String user, String rating, String similarity) {
		this.movie = new Text(movie);
		this.user = new Text(user);
		this.rating = new FloatWritable(Float.valueOf(rating));
		this.similarity = new FloatWritable(Float.valueOf(similarity));
	}
        
	public void readFields(DataInput dataInput) throws IOException {
		movie = new Text();
		user = new Text();
		rating = new FloatWritable();
		similarity = new FloatWritable();

		movie.readFields(dataInput);
		user.readFields(dataInput);
		rating.readFields(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		movie.write(dataOutput);
		user.write(dataOutput);
		rating.write(dataOutput);
		similarity.write(dataOutput);
	}

	// Useful for sorting
	public int compareTo(MovieRatingSimilarity pw) {
		int cmp = this.similarity.compareTo(pw.similarity);
		return cmp;
	}
	
	@Override
	public MovieRatingSimilarity clone() throws CloneNotSupportedException {
		MovieRatingSimilarity mrs = new MovieRatingSimilarity();

		mrs.movie = new Text(this.movie.toString());
		mrs.user = new Text(this.user.toString());
		
		mrs.rating = new FloatWritable(this.rating.get());
		mrs.similarity = new FloatWritable(this.similarity.get());
		
		return mrs;
	}

	@Override
	public String toString() {
		return movie + "," + user+ "," + rating + "," + similarity;
	}
	
	public static MovieRatingSimilarity parse(String line){
		String[] parts = line.split(",");
		return new MovieRatingSimilarity(parts[0],parts[1],parts[2],parts[3]);
	}
	   
}


