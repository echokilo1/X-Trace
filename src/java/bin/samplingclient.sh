#!/bin/bash

BASE=`dirname ${0}`/..

if [ $# != 2 ]; then
    echo -e "Usage: samplingclient.sh [hostname] [samplingpercentage]";
    exit
fi

# Hostname to send sampling percentage
HST=$1;
# The sampling percentage
SAMPLE_PERCENTAGE=$2;
      
CLASSPATH=~/.m2/repository/commons-logging/commons-logging/1.0.3/commons-logging-1.0.3.jar:~/.m2/repository/log4j/log4j/1.2.14/log4j-1.2.14.jar:$BASE/target/xtrace-2.2-11_2011.jar

java -cp $CLASSPATH edu.berkeley.xtrace.samplingserver.SamplingPercentageClient $HST $SAMPLE_PERCENTAGE
