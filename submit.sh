#!/bin/bash

function print_usage {
  echo -e "USAGE\n\tsubmit.sh <input_data_directory> <output_directory>"
  echo -e "\nEXAMPLE\n\tsubmit.sh hdfs://char:30600/cs555/data hdfs://char:30600/cs555/output\n"
}

if [[ $# -eq 2 ]]; then
  echo -e "Submitting Spark Job...\n"
  HADOOP_CONF_DIR="$HOME/hadoop" "${SPARK_HOME}"/bin/spark-submit \
    --class org.movielens.Application \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    target/scala-2.12/movielens_2.12-0.1.jar "$1" "$2"
else
  print_usage
fi

