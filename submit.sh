#!/bin/bash

function print_usage {
  echo -e "USAGE\n\tsubmit.sh <spark_master> <file>"
  echo -e "\nEXAMPLE\n\tsubmit.sh hdfs://cheyenne:30600/cs555/data/movielens\n"
}

if [[ $# -eq 1 ]]; then
  echo -e "Submitting Spark Job...\n"
  "${SPARK_HOME}"/bin/spark-submit \
    --class org.movielens.Application \
    --master yarn \
    --deploy-mode cluster \
    target/scala-2.12/movielens_2.12-0.1.jar "$1"
else
  print_usage
fi

