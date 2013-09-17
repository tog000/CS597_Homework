#!/bin/bash

mvn package
#./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YPR180W YGR063C
./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YGR063C YPR180W
hadoop fs -cat output.txt > tmp
nano tmp
