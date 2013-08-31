package edu.boisestate.cs597;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.Callable;

/**
 * DO NOT DELETE this class The automatic assignment checker will instantiate
 * this class from your jar You may modify the contents of the main method to
 * instantiate and invoke methods on the additional classes that you have
 * created for this assignment.
 * 
 */
public class App {

	private final boolean DEBUG = false;
	
	private String inputFile;
	private String outputFolder;
	private String line;

	// Query 1
	private HashMap<String, PersonVisits> hmap = null;
	
	// Query 2
	private Calendar cal;
	private SimpleDateFormat sdf;
	private Date date;
	private HashMap[] weeks = null; // Cannot parametrize over native array;

	// Inner class for easier HashMap manipulation of values
	private class PersonVisits {
		public String name;
		public Integer visits = 1;
		public PersonVisits(String name) {this.name = name;}
		public void addVisit() {this.visits++;}
	}

	private void readFile(Callable<?> function) {
		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(this.inputFile));
			br.readLine(); // Skip headers
			line = br.readLine();

			while (line != null) {
				function.call();
				line = br.readLine();
			}
			br.close();

		} catch (FileNotFoundException e) {
			System.err.printf("File '%s' not found!\n", this.inputFile);
			System.exit(1);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.printf("Error reading file!\n");
			System.exit(1);
		}
	}

	private void processQuery1() {

		// We invoke the readFile method and pass the query-specific logic (Strategy design pattern) 
		this.readFile(new Callable<Object>(){
				public Object call() throws Exception {
					String[] parts = line.split(",");

					String key = parts[0] + "\t" + parts[1];
					if (hmap.containsKey(key)) {
						hmap.get(key).addVisit();
					} else {
						hmap.put(key, new PersonVisits(key));
					}
					return null;
				}
		});

		ArrayList<PersonVisits> list = new ArrayList<PersonVisits>(hmap.values());

		Collections.sort(list, new Comparator<PersonVisits>() {
			public int compare(PersonVisits person1, PersonVisits person2) {
				return person2.visits.compareTo(person1.visits);
			}
		});
		
		String outputFile = this.outputFolder + "/query_results_1.tsv";
		File file = new File(outputFile);

		try {
			if (!file.exists()) {
				file.createNewFile();
			}

			BufferedWriter bw = new BufferedWriter(new FileWriter(file));

			PersonVisits pv = null;

			for (int i = 0; i < 10; i++) {
				pv = list.get(i);
				bw.write(pv.name + "\t" + pv.visits + "\n");
				
				if(DEBUG)
					System.out.printf("%s\t%d\n", pv.name, pv.visits);
			}

			bw.close();

		} catch (Exception e) {
			System.err.printf("Error writting to output file!\n");
			System.exit(1);
		}

	}

	private void processQuery2() {

		// We invoke the readFile method and pass the query-specific logic (Strategy design pattern)
		this.readFile(new Callable<Object>(){
				public Object call(){
					
					String[] parts = line.split(",");
					if(parts.length > 12){
						try{
							date = sdf.parse(parts[12]);
						}catch(ParseException e){
							// Failed to parse date, return.
							return null;
						}
					}else{
						// If for some reason the date is not there, return.
						return null;
					}
					
					cal.setTime(date);
					
					String key = parts[0] + "\t" + parts[1];
					
					int week = cal.get(Calendar.WEEK_OF_YEAR);
					
					if(weeks[week] == null){
						HashMap<String, PersonVisits> newMap = new HashMap<String, PersonVisits>();
						if(parts.length > 22){
							newMap.put(key, new PersonVisits(parts[21]+" "+parts[22]));
						}else{
							newMap.put(key, new PersonVisits("NO NAME"));
						}
						weeks[week] = newMap;
					}else{
						HashMap<String, PersonVisits> weekHmap = ((HashMap<String, PersonVisits>)weeks[week]);
						if (weekHmap.containsKey(key)) {
							weekHmap.get(key).addVisit();
						} else {
							weekHmap.put(key, new PersonVisits(key));
						}
					}
					
					return null;
				}
		});

		int maxWeek[] = new int[2];
		for(byte i=0;i<weeks.length;i++){
			if(weeks[i] != null && weeks[i].size() > maxWeek[1]){
				maxWeek[0] = i;
				maxWeek[1] = weeks[i].size();
			}
		}
		
		ArrayList<PersonVisits> list = new ArrayList<PersonVisits>(weeks[maxWeek[0]].values());

		Collections.sort(list, new Comparator<PersonVisits>() {
			public int compare(PersonVisits person1, PersonVisits person2) {
				return person2.visits.compareTo(person1.visits);
			}
		});
		
		String outputFile = this.outputFolder + "/query_results_2.tsv";
		File file = new File(outputFile);

		try {
			if (!file.exists()) {
				file.createNewFile();
			}

			BufferedWriter bw = new BufferedWriter(new FileWriter(file));

			PersonVisits pv = null;

			for (int i = 0; i < 10; i++) {
				pv = list.get(i);
				bw.write(pv.name + "\t" + pv.visits + "\n");
				
				if(DEBUG)
					System.out.printf("%s\t%d\n", pv.name, pv.visits);
			}

			bw.close();

		} catch (Exception e) {
			System.err.printf("Error writting to output file!\n");
			System.exit(1);
		}
		
	}

	public App(String inputFile, String outputFolder, int query) {
		this.inputFile = inputFile;
		this.outputFolder = outputFolder;

		// To calculate enlapsed times
		long init = System.currentTimeMillis();
		
		switch (query) {
		
		case 1:
			
			hmap = new HashMap<String, PersonVisits>();
			
			this.processQuery1();
			
			break;
			
		case 2:
			
			this.sdf = new SimpleDateFormat("M/d/y H:m");
			this.cal = Calendar.getInstance();
			this.weeks = new HashMap[54];
			
			this.processQuery2();
			
			break;
		default:
			System.err.println("I don't know that query number!");
			System.exit(1);
			break;
		}
		
		System.out.printf("Query #%d Done.%nEnlapsed %dms%n", query, System.currentTimeMillis() - init);

	}

	public static void main(String[] args) throws Exception {

		//new App("WhiteHouse-WAVES-2012.csv", "./", 1);
		//new App("WhiteHouse-WAVES-2012.csv", "./", 2);
		
		if(args.length < 3){
			System.out.printf("Usage: %s <input file> <output folder> <query number>%n",args[0]);
		}else{
			new App(args[0], args[1], Integer.valueOf(args[2]));
		}

		System.out.println("exiting ...");

	}
}
