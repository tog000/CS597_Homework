visitors = LOAD '$input' using PigStorage(',') as (NAMELAST : chararray,NAMEFIRST: chararray,NAMEMID : chararray,UIN : chararray,BDGNBR : chararray,ACCESS_TYPE : chararray,TOA : chararray,POA : chararray,TOD : chararray,POD : chararray,APPT_MADE_DATE : chararray,APPT_START_DATE : chararray,APPT_END_DATE : chararray,APPT_CANCEL_DATE : chararray,Total_People : chararray,LAST_UPDATEDBY : chararray,POST : chararray,LastEntryDate : chararray,TERMINAL_SUFFIX : chararray,visitee_namelast : chararray,visitee_namefirst : chararray,MEETING_LOC : chararray,MEETING_ROOM : chararray,CALLER_NAME_LAST : chararray,CALLER_NAME_FIRST : chararray,CALLER_ROOM : chararray,Description : chararray,ReleaseDate:chararray);

grouped = GROUP visitors BY (NAMEFIRST, NAMELAST); -- Group visitors by name
result = FOREACH grouped GENERATE group.$0,group.$1,COUNT(visitors) AS total_visits; -- Count the number of items in the bag
result_sorted = ORDER result BY total_visits DESC, $0 ASC; -- Order the results by the count
top_20 = LIMIT result_sorted 20; -- Limit to the top 20

STORE top_20 INTO '$output' using PigStorage('\t');
