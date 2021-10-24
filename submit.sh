#!/bin/bash

function print_usage {
  echo -e "USAGE\n\tsubmit.sh <spark_master> <file>"
  echo -e "\nEXAMPLE\n\tsubmit.sh spark://cheyenne:30633 hdfs://cheyenne:30600/ml-20m/genome-scores.csv\n"
}

if [[ $# -eq 2 ]]; then
  echo -e "Submitting Spark Job...\n"
  "${SPARK_HOME}"/bin/spark-submit \
    --class org.movielens.Application \
    --master "$1" \
    target/scala-2.12/movielens_2.12-0.1.jar "$2"
else
  print_usage
fi

