package edu.boisestate.cs597;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

/**
 * DO NOT DELETE this class
 * The automatic assignment checker will instantiate this class from your jar
 * You may modify the contents of the main method to instantiate and invoke
 * methods on the additional classes that you have created for this assignment.
 *
 */
public class App 
{
	
	private class PersonVisits{
		public String name;
		public Integer visits = 1;
		public PersonVisits(String name){
			this.name=name;
		}
		public void addVisit(){this.visits++;}
	}
	
	public App(String inputFile, String outputFolder){
		HashMap<String, PersonVisits> hmap = new HashMap<String, PersonVisits>();
    	BufferedReader br = null;
        String[] parts = null;
        String key="";
    	
		try {
			br = new BufferedReader(new FileReader(inputFile));
			br.readLine(); // Skip headers
            String line = br.readLine();
            
            while (line != null) {
            	parts=line.split(",");
            	if(parts.length>2){
            		key = (parts[0]+"\t"+parts[1]).toLowerCase();
            		if(hmap.containsKey(key)){
            			hmap.get(key).addVisit();
            		}else{
            			hmap.put(key,new PersonVisits(key));
            		}
            	}
                line = br.readLine();
            }
            br.close();
            
		} catch (FileNotFoundException e) {
			System.err.printf("File '%s' not found!\n",inputFile);
			System.exit(1);
		} catch (Exception e) {
			System.err.printf("Error reading file!\n");
			System.exit(1);
        }
    
		ArrayList<PersonVisits> list = new ArrayList<PersonVisits>(hmap.values());
		
		Collections.sort(list, new Comparator<PersonVisits>(){
			public int compare(PersonVisits person1, PersonVisits person2){
				return person2.visits.compareTo(person1.visits);
			}
		});
		String outputFile = outputFolder+"/query_results_1.tsv";
		File file = new File(outputFile);
		
		try{
			if(!file.exists()) {
			    file.createNewFile();
			}
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			
			PersonVisits pv = null;
			
			for(int i=0;i<10;i++){
				pv = list.get(i);
				bw.write(pv.name+"\t"+pv.visits+"\n");
				//System.out.printf("%s\t%d\n",pv.name,pv.visits);
			}
			
			bw.close();
			
		} catch (Exception e) {
			System.err.printf("Error writting to output file!\n");
			System.exit(1);
	    }
		
		
	}

    public static void main( String[] args ) throws IOException
    {
    	
    	new App("WhiteHouse-WAVES-2012.csv","./");
    	
		System.out. println("exiting ...");
		
		
		
    }
}
