Assignment 2
============

The numbers in numbered lists are suffixed with an M or an R depending
if they belong to a Map or a Reduce job. 


Problem I
=========


Query #1
--------

This is a straight forward query, except for extracting the top 20, which is 
a functionality that is not built into Hadoop.

1M. VisitorsMapper extracts all the visitor names from the input and
writes the key-value pair (LASTNAME \t FIRSTNAME,1)

2R. VisitorsReducer adds all the occurrences of unique visitors and creates
a list of 20 objects type VisitorCount. On each new key-value pair, it
compares the visits to the top list and accommodates it accordingly.

In the cleanup section, the top 20 results are written back to disk.

Query #2
--------

1M. `BestWeekMapper` extracts the visitor and visitee names and week number 
and stores them in a `VisitWritable` object. 
The key-value pair generated is (LASTNAME \t FIRSTNAME,VisitWritable)

1R. `BestWeekReducer` extracts the week number from the and swaps it with key 
and saves to disk using the SequenceFile format

2M. An Identity Mapper is used

2R. `BestWeekVisiteesReducer` saves to disk the key-value pair 
(VISITEEFIRST \t VISITEELAST, FIRSTNAME \t LASTNAME) on a different file for 
each different key (week number), and keeps a running total of the best week 
that gets saved to a different file called weekTotals. Saves to disk using 
the `SequenceFile` format.

The weekTotals file is read and the best week identified. Job 3 gets all the files 
that match the week number as input.

3M. An Identity Mapper is used, and because VISITEE is the key, the reducer in 3R
can easily calculate the total visitors for each visitee.

3R. The `BestWeekVisiteesCountReducer` moves the visitor count to the key by putting
it into a `TextIntWritable` (an object with a Text and IntWritable parts)
and writes to disk using the SequenceFile format.

4M. An Identity Mapper is used.

4R. A `SortComparator` is configured, so a custom strategy for sorting can be
implemented. The class `KeyComparator` examines every key and sorts first by visit
count, and if the count is the same, falls back to sorting by VISITEE name.
Extracts from the key (`TextIntWritable`) the visitee name and the count, and writes
to disk using the regular text output.


Problem II
==========

My solution for this problem is correct, but is not based following the description
of the algorithm found in section 2.3.1 of Mining of Massive Datasets, since their
implementation requires the vector or matrix A to fit in memory, and to be
loaded on every mapper, something that I think is not optimal.


1M. The program determines which matrix is operating on.
 
A) If its operating on the left matrix, it will write to context every number 
found on the current line `MATRIXB_HEIGHT` times. This is the number of times 
such number will be used to calculate the resulting matrix.

B) If its operating on the right matrix, it will output every number found on the
current line `MATRIXA_WIDTH` times, for similar reasons.

The Mapper then needs to write a key that represents the target position in the 
destination matrix, and to calculate that number:

A) If operating on left matrix, it uses the line number as the Y coordinate, and
because we are writing the same value multiple times (up to MATRIXB_HEIGHT), 
uses the current iteration number as the X coordinate of the final matrix.

B) If operating on right matrix, it uses the iteration number as the Y coordinate, 
and the current column the X coordinate of the final matrix.

Finally, we want to have these numbers ordered so we can multiply and add them without
to having to sort them on the reducer, so a number is added to the key to keep the
order:

A) The key is appended the (current column number * 2)

B) The key is appended the (current line number * 2) + 1


1R. By implementing a `GroupingComparator`, we can make sure that certain keys arrive at 
the same reducer, so I implemented a partial key comparator `PartialKeyComparator` to this
effect. The comparator will look at the first two numbers in the key (Y and X coordinates)
and ignore the last part. By doing this we are guaranteed that the elements will be in the
right order due to the sort at the output of the mapper.

The Reducer then only needs to multiply the first two items and add them to a running total
and repeat for each pair of values on the input, and when its done, write back the result.

If the Y coordinate is different than 0 and the X coordinate is 0, it means that we need to
insert a new line.
