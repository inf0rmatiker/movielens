#!/bin/bash

echo -e "\nExecuting $ ${HADOOP_HOME}/bin/hadoop fs -rm -r /cs555/output && ${HADOOP_HOME}/bin/hadoop fs -mkdir /cs555/output\n"
${HADOOP_HOME}/bin/hadoop fs -rm -r /cs555/output && ${HADOOP_HOME}/bin/hadoop fs -mkdir /cs555/output
