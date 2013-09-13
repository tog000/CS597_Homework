package edu.boisestate.cs597;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ProteinWritable implements WritableComparable<ProteinWritable>{
	
	public IntWritable id;
	public Text name;
	public IntWritable getId() {
		return id;
	}
	public void setId(IntWritable id) {
		this.id = id;
	}
	public Text getName() {
		return name;
	}
	public void setName(Text name) {
		this.name = name;
	}
	public ProteinWritable() {
		this.id = new IntWritable();
		this.name = new Text();
	}
	public ProteinWritable(int id, String name) {
		this.id = new IntWritable(id);
		this.name = new Text(name);
	}
	public ProteinWritable(IntWritable id, Text name) {
		this.id = id;
		this.name = name;
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		id = new IntWritable();
		name = new Text();
		
		id.readFields(dataInput);
		name.readFields(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		id.write(dataOutput);
		name.write(dataOutput);
	}
	
	// Useful for sorting
	public int compareTo(ProteinWritable pw) {
		
		int cmp = this.id.compareTo(pw.id);
		if(cmp==0){
			return name.getLength() - pw.name.getLength();
		}
		return cmp;
	}

}
