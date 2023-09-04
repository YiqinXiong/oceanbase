OB_PATH='/data/2/xiongyiqin.xyq/workspace/oceanbase'
OBD_PATH=$OB_PATH/tools/deploy
OBSERVER_PATH='/data/0/xiongyiqin.xyq/observer1/bin'

echo "[1/4] $OBD_PATH/obd.sh stop -n ob1"
$OBD_PATH/obd.sh stop -n ob1 || exit -1

if [ -z "$1" ]; then  # 判断第一个参数是否为空
    echo "[2/4] ** copy observer from build output **"
    cp $OB_PATH/build_debug/src/observer/observer $OBD_PATH/bin/observer || exit -2
else
    echo "[2/4] ** copy observer from $1 **"
    cp $1 $OBD_PATH/bin/observer || exit -2
fi

echo "[3/4] cp $OBD_PATH/bin/observer $OBSERVER_PATH/observer"
cp $OBD_PATH/bin/observer $OBSERVER_PATH/observer || exit -3

echo "[4/4] $OBD_PATH/obd.sh start -n ob1"
$OBD_PATH/obd.sh start -n ob1 || exit -4

echo "all finished successfully!"
