if [ -d data/query_results_2 ]
then
        echo "It exists, removing"
        rm -r data/query_results_2
fi

pig -x local -param input=data/white.csv -param output=data/query_results_2 scripts/script-2.pig
