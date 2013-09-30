package edu.boisestate.cs597;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class WeekdaysBetweenDates extends EvalFunc<Integer> {
	public Integer exec(Tuple input) throws IOException {
		Integer error = -1;
		if (input == null || input.size() == 0) {
			return null;
		}
		if ( input.size() == 2 ){
			String date1 = (String) input.get(0);
			String date2 = (String) input.get(1);
			try {
				SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy");
				Date dateStart = format.parse(date1);
				Date dateEnd = format.parse(date2);
				
				Calendar cal = Calendar.getInstance();
				cal.setTime(dateStart);
				Integer day1 = cal.get(Calendar.DAY_OF_WEEK);
				Integer dayOfYear1 = cal.get(Calendar.DAY_OF_YEAR);
				cal.setTime(dateEnd);
				Integer day2 = cal.get(Calendar.DAY_OF_WEEK);
				Integer dayOfYear2 = cal.get(Calendar.DAY_OF_YEAR);
				
				Integer weekDays = 0;
				
				for(int i=dayOfYear1;i<dayOfYear2;i++){
					if(day1 != Calendar.SUNDAY && day1 != Calendar.SATURDAY){
						weekDays += 1;
					}
					day1+=1;
					if(day1==8){
						day1 = 1;
					}
				}
				
				return weekDays;
				
			} catch (Exception e) {
				return 0;
			}
		}
		return 0;
		
	}
	
}