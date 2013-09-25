package edu.boisestate.cs597;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ProteinWritable implements WritableComparable<ProteinWritable>, Cloneable{
	
	public IntWritable id;
	public Text name;
	public Text path;
	public IntWritable pathLength;
	
	public IntWritable getId() {
		return id;
	}
	public void setId(int id) {
		this.id = new IntWritable(id);
	}
	public Text getName() {
		return name;
	}
	public void setName(String name) {
		this.name = new Text(name);
	}
	public Text getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = new Text(path);
	}
	
	public void appendToPath(int id) {
		this.path.set(this.path.toString()+Integer.valueOf(id).toString()+",");
		incrementPathLength();
	}
	
	public IntWritable getPathLength() {
		return pathLength;
	}
	public void setPathLength(IntWritable pathLength) {
		this.pathLength = pathLength;
	}
	
	public void incrementPathLength() {
		this.pathLength.set(this.pathLength.get()+1);
	}
	
	public ProteinWritable() {
		this.id = new IntWritable();
		this.name = new Text();
		this.path = new Text();
		this.pathLength = new IntWritable();
	}
	public ProteinWritable(int id, String name) {
		this.id = new IntWritable(id);
		this.name = new Text(name);
		this.path = new Text();
		this.pathLength = new IntWritable();
	}
	public ProteinWritable(IntWritable id, Text name) {
		this.id = id;
		this.name = name;
		this.path = new Text();
		this.pathLength = new IntWritable();
	}
	
	public void readFields(DataInput dataInput) throws IOException {
		id = new IntWritable();
		name = new Text();
		path = new Text();
		pathLength = new IntWritable();
		
		id.readFields(dataInput);
		name.readFields(dataInput);
		path.readFields(dataInput);
		pathLength.readFields(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		id.write(dataOutput);
		name.write(dataOutput);
		path.write(dataOutput);
		pathLength.write(dataOutput);
	}
	
	// Useful for sorting
	public int compareTo(ProteinWritable pw) {
		
		int cmp = this.id.compareTo(pw.id);
		if(cmp==0){
			if(pw.name.getLength() == 0 || name.getLength() == 0){
				return pw.name.getLength() - name.getLength();
			}
			return name.compareTo(pw.name);
		}
		return cmp;
	}

	@Override
	protected ProteinWritable clone() throws CloneNotSupportedException{
		ProteinWritable pw = new ProteinWritable();
		pw.id = new IntWritable(this.id.get());
		pw.name = new Text(this.name.toString());
		pw.path = new Text(this.path.toString());
		pw.pathLength = new IntWritable(this.pathLength.get());
		return pw;
	}
	
}
