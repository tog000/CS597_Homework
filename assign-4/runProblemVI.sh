if [ -d data/query_results_3 ]
then
        echo "It exists, removing"
        rm -r data/query_results_3
fi

pig -x local -param input=data/white.csv -param output=data/query_results_3 scripts/script-3.pig
