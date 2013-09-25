#!/bin/bash

mvn package
#./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YBR236C YOR151C # One hop
#./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YPR180W YGR063C # Two hops
#./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YGR063C YPR180W # Impossible
#./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YNR016C YPR178W # Many hops
./runProblemIII.sh input/nodes.txt input/edges.txt output.txt YBR236C YIL061C # Super long
hadoop fs -cat output.txt > tmp
nano tmp
