#!/bin/bash

if [ $# -le 2 ]
then
        echo  "Usage: $0 <input path> <output path> <input file>  [rebuild]"
else
        if [ $# -eq 4 ]
        then
                mvn package;
        fi

	input=$1
	output=$2;
	input_file=$3;

        echo "************ GET ALL MOVIES SEEN BY CUSTOMER ************"
        hadoop jar target/final-0.0.1-SNAPSHOT.jar edu.boisestate.cs597.App $input $output $input_file
	hadoop dfs -cat output/part* > output.txt
fi

