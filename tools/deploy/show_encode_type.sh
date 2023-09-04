#! /bin/bash
# parameter 1: loop times
OBIP="127.0.0.1"
OBPORT="43700"
OBUSER="root@mytpch"
OBDATABASE="test"
OB="/data/2/xiongyiqin.xyq/workspace/oceanbase/deps/3rd/u01/obclient/bin/obclient -h $OBIP -P $OBPORT"
OB_TPC="$OB -u$OBUSER -D$OBDATABASE -f -c -A -s"
# TEST_SQL="echo \"select * from t1;\""
#TEST_SQL="cat /home/xiongyiqin.xyq/downloads/tpc-test/tpc-tool/tpch-tool/queries-mysql/q01.sql"
#TEST_SQL="echo select p_brand, p_type, p_retailprice from part where p_retailprice in (1800,1900,2000);"
# TEST_SQL='echo select p_brand, p_type, p_retailprice from part where p_type in ("SMALL BRUSHED COPPER", "SMALL BRUSHED COPPDR", "MEDIUM POLISHED STEEL");'
TEST_SQL="select 1 from $1 where $2 in (NULL, NULL); select last_trace_id() from dual;"
trace_id=$(echo $TEST_SQL | $OB_TPC)
echo $trace_id
./encode_type.sh $trace_id | grep -o "column_decoder.*result"
