package edu.boisestate.cs597;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class VisitWritable implements WritableComparable<VisitWritable>{

	public IntWritable week;
	public Text visitor;
	public Text visitee;
	
	public VisitWritable(){
		this.week = new IntWritable();
		this.visitor=new Text();
		this.visitee=new Text();
	}
	
	public VisitWritable(int week,String visitor, String visitee){
		this.week = new IntWritable(week);
		this.visitor=new Text(visitor);
		this.visitee=new Text(visitee);
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		week = new IntWritable();
		visitor = new Text();
		visitee = new Text();
		
		week.readFields(dataInput);
		visitor.readFields(dataInput);
		visitee.readFields(dataInput);
		
	}

	public void write(DataOutput dataOutput) throws IOException {
		week.write(dataOutput);
		visitor.write(dataOutput);
		visitee.write(dataOutput);
	}

	public int compareTo(VisitWritable vw) {
		return this.week.compareTo(vw.week);
	}

}
