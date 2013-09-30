A = LOAD '$input' using PigStorage(',') as (NAMELAST : chararray,NAMEFIRST : chararray,NAMEMID : chararray,UIN : chararray,BDGNBR : chararray,ACCESS_TYPE : chararray,TOA : chararray,POA : chararray,TOD : chararray,POD : chararray,APPT_MADE_DATE : chararray,APPT_START_DATE : chararray,APPT_END_DATE : chararray,APPT_CANCEL_DATE : chararray,Total_People : chararray,LAST_UPDATEDBY : chararray,POST : chararray,LastEntryDate : chararray,TERMINAL_SUFFIX : chararray,visitee_namelast : chararray,visitee_namefirst : chararray,MEETING_LOC : chararray,MEETING_ROOM : chararray,CALLER_NAME_LAST : chararray,CALLER_NAME_FIRST : chararray,CALLER_ROOM : chararray,Description : chararray,ReleaseDate:chararray);


STORE A INTO '$output' using PigStorage('\t');
