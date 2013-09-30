if [ -d data/query_results_1 ]
then
	echo "It exists, removing"
	rm -r data/query_results_1
fi
pig -x local -param input=data/white.csv -param output=data/query_results_1 scripts/script-1.pig
