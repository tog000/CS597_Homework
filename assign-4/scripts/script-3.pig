register 'target/assign4-1.0-SNAPSHOT.jar';

visitors = LOAD '$input' using PigStorage(',') as (NAMELAST : chararray,NAMEFIRST : chararray,NAMEMID : chararray,UIN : chararray,BDGNBR : chararray,ACCESS_TYPE : chararray,TOA : chararray,POA : chararray,TOD : chararray,POD : chararray,APPT_MADE_DATE : chararray,APPT_START_DATE : chararray,APPT_END_DATE : chararray,APPT_CANCEL_DATE : chararray,Total_People : chararray,LAST_UPDATEDBY : chararray,POST : chararray,LastEntryDate : chararray,TERMINAL_SUFFIX : chararray,visitee_namelast : chararray,visitee_namefirst : chararray,MEETING_LOC : chararray,MEETING_ROOM : chararray,CALLER_NAME_LAST : chararray,CALLER_NAME_FIRST : chararray,CALLER_ROOM : chararray,Description : chararray,ReleaseDate:chararray);

grouped = GROUP visitors BY (NAMEFIRST, NAMELAST); -- Group visitors by name

-- Nested for loop
result = FOREACH grouped {

		-- visitors is a bag of records for this group
		b = FOREACH visitors GENERATE edu.boisestate.cs597.WeekdaysBetweenDates(APPT_MADE_DATE, APPT_START_DATE); -- Calculate the weekdays between the dates
		GENERATE group.$0,group.$1, AVG(b) as average; -- Output the grouping columns and the average
}

result_sorted = ORDER result BY average DESC; -- Order the results by the count
top_20 = LIMIT result_sorted 20; -- Limit to the top 20

STORE top_20 INTO '$output' using PigStorage('\t');
