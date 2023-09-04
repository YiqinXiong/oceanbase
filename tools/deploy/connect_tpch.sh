OBC_PATH=/data/2/xiongyiqin.xyq/workspace/oceanbase/deps/3rd/u01/obclient/bin
CON_IP=127.0.0.1
CON_PORT=43700
CON_USER=root
CON_TENANT=mysql
CON_DB=xyq

$OBC_PATH/obclient -h $CON_IP -P $CON_PORT -u $CON_USER@$CON_TENANT -D $CON_DB -f -c -A -vvv
