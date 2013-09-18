#!/bin/bash

mvn package
#./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YPR180W YGR063C
#./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YGR063C YPR180W
./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YNR016C YPR178W
hadoop fs -cat output.txt > tmp
nano tmp
