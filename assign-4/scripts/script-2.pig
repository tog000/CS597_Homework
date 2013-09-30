register 'target/assign4-1.0-SNAPSHOT.jar';

visitors = LOAD '$input' using PigStorage(',') as (NAMELAST : chararray,NAMEFIRST : chararray,NAMEMID : chararray,UIN : chararray,BDGNBR : chararray,ACCESS_TYPE : chararray,TOA : chararray,POA : chararray,TOD : chararray,POD : chararray,APPT_MADE_DATE : chararray,APPT_START_DATE : chararray,APPT_END_DATE : chararray,APPT_CANCEL_DATE : chararray,Total_People : chararray,LAST_UPDATEDBY : chararray,POST : chararray,LastEntryDate : chararray,TERMINAL_SUFFIX : chararray,visitee_namelast : chararray,visitee_namefirst : chararray,MEETING_LOC : chararray,MEETING_ROOM : chararray,CALLER_NAME_LAST : chararray,CALLER_NAME_FIRST : chararray,CALLER_ROOM : chararray,Description : chararray,ReleaseDate:chararray);

weeks = FOREACH visitors GENERATE edu.boisestate.cs597.WeekOfYear(APPT_START_DATE) as week_number, NAMEFIRST, NAMELAST, APPT_START_DATE; -- Calculate in which week each visit happened

weeks_grouped = GROUP weeks BY week_number; -- Group by week number

result = FOREACH weeks_grouped {
	-- weeks is a bag of the visitors this week
	b = DISTINCT weeks; -- Get all the distinct visitors (unique)
	GENERATE group,COUNT(b) AS total_visitors; -- Count the unique visitors
}

result_sorted = ORDER result BY total_visitors DESC; -- Order the results by the count

max_week = LIMIT result_sorted 1; -- Limit to the highest week

visitors_max_week = FILTER visitors BY edu.boisestate.cs597.WeekOfYear(APPT_START_DATE) == max_week.group; -- Filter the visitors by those who visited in the max_week

visitors_max_week_grouped = GROUP visitors_max_week BY (visitee_namefirst, visitee_namelast); -- Group by visitee name

result = FOREACH visitors_max_week_grouped GENERATE group.$0,group.$1,COUNT(visitors_max_week) AS total_visits; -- Calculate the number of visitors for each visitee

result_sorted = ORDER result BY total_visits DESC, $0 ASC; -- Order the results by the count

STORE result_sorted INTO '$output' using PigStorage('\t');
