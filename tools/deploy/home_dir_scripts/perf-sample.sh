#!/bin/bash
# 1 parameter: collect time (in seconds)
if [ -z $1 ]
then
    echo "MISSING ARGUMENT: collect time (in seconds)"
    exit -1
fi
PID=$(ps -aux | grep /bin/observer | grep -v "grep" | awk '{print $2}')
WDIR="/data/2/xiongyiqin.xyq/workspace"
if [ ${#PID} -eq 0 ]
then
    echo "observer is not running"
    exit -1
fi
#COM1="perf record -o perf-sample.data -e cycles -c 100000000 -g -p $PID -- sleep $1"
COM1="perf record -o perf-sample.data -F 99 -g -p $PID -- sleep $1"
COM2="perf script -i ~/perf-sample.data -F ip,sym -f > ~/perf-sample.viz"
echo $COM1
eval $COM1
echo $COM2
eval $COM2

echo "check result: http://$(hostname -i):39411/perf-sample.viz"
