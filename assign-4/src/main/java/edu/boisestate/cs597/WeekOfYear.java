package edu.boisestate.cs597;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class WeekOfYear extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		Integer error = -1;
		if (input == null || input.size() == 0) {
			return null;
		}
		String str = (String) input.get(0);
		try {
			SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy");
			Date date = format.parse(str);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			Integer week = cal.get(Calendar.WEEK_OF_YEAR);
			return week.toString();
		} catch (Exception e) {
			return error.toString();
		}
		
	}
}