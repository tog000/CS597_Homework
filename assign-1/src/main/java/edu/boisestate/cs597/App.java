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
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

/**
 * @author Gabriel Trisca
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
	private HashMap[] weeks = null; // Cannot parametrize on native array;

	// Inner class for easier HashMap manipulation of values
	public class PersonVisits {
		public String name;
		public Integer visits = 1;
		public PersonVisits(String name) {this.name = name;}
		public void addVisit() {this.visits++;}
	}
	
	
	/**
	 * @param function Callable that gets executed for every line read from input
	 * 
	 */
	private void readFile(Callable<?> function) {
		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(this.inputFile));
			br.readLine(); // Skip headers
			line = br.readLine();

			while (line != null) {
				// Invoke function, this allows for better code reuse
				function.call();
				// Fetch next line
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

	/**
	 * Process query 1
	 */
	private void processQuery1() {

		// We invoke the readFile method and pass the query-specific logic (Strategy design pattern) 
		this.readFile(new Callable<Object>(){
				public Object call() throws Exception {
					String[] parts = line.split(",");

					String key = parts[0] + "\t" + parts[1];
					
					// If the HashMap already contains the visitor name
					if (hmap.containsKey(key)) {
						// Increase the visit number
						hmap.get(key).addVisit();
					} else {
						// Add visitor to map
						hmap.put(key, new PersonVisits(key));
					}
					return null;
				}
		});

		// Sort the map
		ArrayList<PersonVisits> list = new ArrayList<PersonVisits>(hmap.values());
		Collections.sort(list, new Comparator<PersonVisits>() {
			public int compare(PersonVisits person1, PersonVisits person2) {
				return person2.visits.compareTo(person1.visits);
			}
		});
		
		
		// Generate output file
		String outputFile = this.outputFolder + "/query_results_1.tsv";
		File file = new File(outputFile);

		try {
			if (!file.exists()) {
				file.createNewFile();
			}

			BufferedWriter bw = new BufferedWriter(new FileWriter(file));

			PersonVisits pv = null;

			for (int i = 0; i < 20; i++) {
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
	
	/**
	 * Process query 2
	 * @param writeFile should the output be written to an output file (true by default)
	 * @return HashMap of the most popular week, key is `String` visitee name, value is `Integer` number of visits
	 */
	private HashMap<String, PersonVisits> processQuery2() {return processQuery2(true);}
	private HashMap<String, PersonVisits> processQuery2(boolean writeFile) {

		// We invoke the readFile method and pass the query-specific logic (Strategy design pattern)
		this.readFile(new Callable<Object>(){
				public Object call(){
					
					// Split line
					String[] parts = line.split(",");
					if(parts.length > 12){
						try{
							// Try to parse the date
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
					
					// Generate key with visitor name
					String key = parts[0] + "\t" + parts[1];

					// Extract week of the year
					int week = cal.get(Calendar.WEEK_OF_YEAR);
					
					PersonVisits visitee = null;
					
					// Some lines are incomplete, filter
					if(parts.length > 22){
						visitee = new PersonVisits(parts[21]+" "+parts[22]);
					}else{
						visitee = new PersonVisits("NO NAME");
					}
					
					// If week doesn't have any visitors yet
					if(weeks[week] == null){
						// Initialize HashMap for the week
						HashMap<String, LinkedList<PersonVisits>> newMap = new HashMap<String, LinkedList<PersonVisits>>();
						
						// Create a list of visitees for this particular visitor
						LinkedList<PersonVisits> list = new LinkedList<PersonVisits>();
						
						// Add visitee to list
						list.add(visitee);
						
						// Add list to map 
						newMap.put(key, list);
						
						// Place the HashMap on the week array
						weeks[week] = newMap;
					}else{
						// We've seen this week before, append
						HashMap<String, LinkedList<PersonVisits>> weekHmap = ((HashMap<String, LinkedList<PersonVisits>>)weeks[week]);
						if (weekHmap.containsKey(key)) {
							weekHmap.get(key).add(visitee);
						} else {
							LinkedList<PersonVisits> list = new LinkedList<PersonVisits>();
							list.add(visitee);
							weekHmap.put(key, list);
						}
					}
					
					return null;
				}
		});

		// Find the best week out there
		int maxWeek[] = new int[2];
		for(byte i=0;i<weeks.length;i++){
			if(weeks[i] != null && weeks[i].size() > maxWeek[1]){
				maxWeek[0] = i;
				maxWeek[1] = weeks[i].size();
			}
		}
		
		// We need to flip the key, now the visitees are the key (This what Hadoop is good for, feel like I'm wasting my time here)
		
		// Initialize map
		HashMap<String, PersonVisits> visiteeMap = new HashMap<String, PersonVisits>();
		// For each visitor->visitee list
		for(LinkedList<PersonVisits> visitees : ((HashMap<String, LinkedList<PersonVisits>>)weeks[maxWeek[0]]).values()){
			// For each visitee, add it to the map and increase visit count 
			for(PersonVisits visitee : visitees){
				if(visiteeMap.containsKey(visitee.name)){
					visiteeMap.get(visitee.name).addVisit();; // Increase by one
				}else{
					visiteeMap.put(visitee.name,new PersonVisits(visitee.name)); // Set to one
				}
			}
		}
		
		// Sort the map
		ArrayList<PersonVisits> list = new ArrayList<PersonVisits>(visiteeMap.values());
		Collections.sort(list, new Comparator<PersonVisits>() {
			public int compare(PersonVisits person1, PersonVisits person2) {
				return person2.visits.compareTo(person1.visits);
			}
		});
		
		
		// Only proceed if we really want to save to file
		if(writeFile){
			// Generate output file 
			String outputFile = this.outputFolder + "/query_results_2.tsv";
			File file = new File(outputFile);
	
			try {
				if (!file.exists()) {
					file.createNewFile();
				}
	
				BufferedWriter bw = new BufferedWriter(new FileWriter(file));
	
				for(PersonVisits visitee : list){
					bw.write(visitee.name + "\t" + visitee.visits + "\n");
					if(DEBUG)
						System.out.printf("%s\t%d\n", visitee.name, visitee.visits);
				}
	
				bw.close();
	
			} catch (Exception e) {
				System.err.printf("Error writting to output file!\n");
				System.exit(1);
			}
		}
		
		// Return the HashMap of the best week (For query 3)
		return visiteeMap;
		
	}

	public App(String inputFile, String outputFolder, int query) {
		this.inputFile = inputFile;
		this.outputFolder = outputFolder;

		// To calculate enlapsed times
		long init = System.currentTimeMillis();
		
		switch (query) {
		
		case 1:
			
			// Initialize
			hmap = new HashMap<String, PersonVisits>();
			
			processQuery1();
			
			break;
			
		case 2:
			
			// Initialize structures for date/week handling
			this.sdf = new SimpleDateFormat("M/d/y H:m");
			this.cal = Calendar.getInstance();
			this.weeks = new HashMap[54];
			
			processQuery2();
			
			break;
		case 3: // Case 3 is just one step further from step 2

			// Initialize structures for date/week handling
			this.sdf = new SimpleDateFormat("M/d/y H:m");
			this.cal = Calendar.getInstance();
			this.weeks = new HashMap[54];
			
			//maxWeek = processQuery2(false);
			
			// Print all the pairs
			
			break;
		default:
			System.err.println("I don't know that query number!");
			System.exit(1);
			break;
		}
		
		System.out.printf("Query #%d Done.%nEnlapsed %dms%n", query, System.currentTimeMillis() - init);

	}

	public static void main(String[] args) throws Exception {
		
		/**/
			//new App("WhiteHouse-WAVES-2012.csv", "./", 1);
			new App("WhiteHouse-WAVES-2012.csv", "./", 2);
		/*/
		if(args.length < 3){
			System.out.printf("Usage: ./run.sh <input file> <output folder> <query number>%n");
		}else{
			new App(args[0], args[1], Integer.valueOf(args[2]));
		}
		/**/

		System.out.println("exiting ...");

	}
}
