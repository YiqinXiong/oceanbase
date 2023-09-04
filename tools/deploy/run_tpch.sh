OB_HOME=/data/2/xiongyiqin.xyq/workspace/oceanbase
OBD_PATH=$OB_HOME/tools/deploy
SQL_PATH=$OBD_PATH/simple_sql
TPCH_CLUSTER=ob1
TPCH_TENANT=mytpch
TPCH_SF=10

if [ -z "$1" ]; then
    echo "use default --sql-path=$SQL_PATH"
else
    SQL_PATH=$1
fi


$OBD_PATH/obd.sh tpch -n $TPCH_CLUSTER --tenant=$TPCH_TENANT -s $TPCH_SF --test-only --optimization=0 --tmp-dir=$SQL_PATH/log --sql-path=$SQL_PATH
