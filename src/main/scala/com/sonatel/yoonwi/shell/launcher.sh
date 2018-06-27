#!/bin/bash
source $1
spark-submit --jars $JARS_FILE --master $MASTER --class $CLASS --executor-memory $EXECUTOR_MEMORY --total-executor-cores $TOTAL_EXECUTOR_CORES $COUNTER_JAR_FILE $CONF_JAR_FILE