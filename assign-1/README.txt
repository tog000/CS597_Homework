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
but that something differs about them (what to do with the
lines).

A `Callable` is passed to the `readFile` function that 
takes care of the query-specific logic.


Additional Query
----------------

This software includes a query #3 that computes the frequency
of visits per every day of the week for the busiest week
of the year. The output of this query is what was used
to generate the following visualization:

http://tog000.com.ar/demos/bigdata/whitehouse/








   

