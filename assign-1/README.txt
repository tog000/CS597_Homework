Assignment 1 (Gabriel Trisca)
============

This assignment required developing at least two different 
algorithms for the queries and a common `read` function to
process the input.

I used the Strategy Pattern for abstracting the 
functionality of the `read` and allowing it to be generic
for both queries.

The Strategy Pattern defines algorithms that are similar
in their general characteristics (open file, read lines)
but that something differs about them (what to do with 
the lines).

A `Callable` is passed to the `readFile` function that 
takes care of the query-specific logic.

Threaded Version
----------------

Since reading the files is probably the most time consuming
part of this assignment, one can imagine that the bottle-neck
is on the I/O side and not on the computation side.

The computational part comprises only of populating HashMaps 
and traversing them.

Implementing a threaded version wouldn't improve the perfor-
mance beyond that of the slowest part of the software: 
reading files.

Additional Query
----------------

This software includes a query #3 that computes the frequen-
cy of visits per every day of the week for the busiest week
of the year. The output of this query is what was used
to generate the following visualization:

http://tog000.com.ar/demos/bigdata/whitehouse/








   

