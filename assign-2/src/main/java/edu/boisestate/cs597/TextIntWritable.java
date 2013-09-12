package edu.boisestate.cs597;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextIntWritable implements WritableComparable<TextIntWritable>{

	public Text text;
	public IntWritable number;
	
	public TextIntWritable(){
		this.text   = new Text();
		this.number = new IntWritable();
	}
	
	public TextIntWritable(String text,int number){
		this.text	= new Text(text);
		this.number = new IntWritable(number);
	}
	
	public TextIntWritable(Text text,int number){
		this.text	= text;
		this.number = new IntWritable(number);
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		text	= new Text();
		number  = new IntWritable();
		
		text.readFields(dataInput);
		number.readFields(dataInput);
		
	}

	public void write(DataOutput dataOutput) throws IOException {
		text.write(dataOutput);
		number.write(dataOutput);
	}

	public int compareTo(TextIntWritable vw) {
		return this.number.compareTo(vw.number);
	}

}
