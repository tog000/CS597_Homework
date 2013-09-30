register 'target/assign4-1.0-SNAPSHOT.jar';

visitors = LOAD '$input' using PigStorage(',') as (NAMELAST : chararray,NAMEFIRST : chararray,NAMEMID : chararray,UIN : chararray,BDGNBR : chararray,ACCESS_TYPE : chararray,TOA : chararray,POA : chararray,TOD : chararray,POD : chararray,APPT_MADE_DATE : chararray,APPT_START_DATE : chararray,APPT_END_DATE : chararray,APPT_CANCEL_DATE : chararray,Total_People : chararray,LAST_UPDATEDBY : chararray,POST : chararray,LastEntryDate : chararray,TERMINAL_SUFFIX : chararray,visitee_namelast : chararray,visitee_namefirst : chararray,MEETING_LOC : chararray,MEETING_ROOM : chararray,CALLER_NAME_LAST : chararray,CALLER_NAME_FIRST : chararray,CALLER_ROOM : chararray,Description : chararray,ReleaseDate:chararray);

weeks = FOREACH visitors GENERATE APPT_START_DATE, edu.boisestate.cs597.WeekOfYear(APPT_START_DATE) as week_number;

weeks_grouped = GROUP weeks BY week_number;

result = FOREACH weeks_grouped GENERATE group,COUNT(weeks) AS total_visitors;

result_sorted = ORDER result BY total_visitors DESC, $0 ASC;

max_week = LIMIT result_sorted 1;

visitors_max_week = FILTER visitors BY edu.boisestate.cs597.WeekOfYear(APPT_START_DATE) == max_week.group;

visitors_max_week_grouped = GROUP visitors_max_week BY (visitee_namefirst, visitee_namelast);

result = FOREACH visitors_max_week_grouped GENERATE group.$0,group.$1,COUNT(visitors_max_week) AS total_visits;

result_sorted = ORDER result BY total_visits DESC, $0 ASC;

STORE result_sorted INTO '$output' using PigStorage('\t');
