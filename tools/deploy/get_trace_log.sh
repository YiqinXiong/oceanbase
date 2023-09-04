OUTPUT_PATH="/data/2/xiongyiqin.xyq/workspace/oceanbase/xyq_trace_log"
DB_PATH="/data/0/xiongyiqin.xyq/observer1"
TID="$1"
OUTPUT_NAME="$2"

if [ -z $2 ] 
then
    grep "$TID" $DB_PATH/log/observer.log* | vim -
else
    grep "$TID" $DB_PATH/log/observer.log* > $OUTPUT_PATH/$OUTPUT_NAME.log
fi

