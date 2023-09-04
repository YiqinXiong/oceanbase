import subprocess

# 连接obclient
OB_CLIENT_PATH="/data/2/xiongyiqin.xyq/workspace/oceanbase/deps/3rd/u01/obclient/bin/obclient"
OB_IP="127.0.0.1"
OB_PORT="43700"
OB_USER="root@mytpch"
OB_DB="xyq"
OB_ARGS="-f -c -A -s"
OB_CLIENT_CONN=f"{OB_CLIENT_PATH} -h {OB_IP} -P {OB_PORT} -u {OB_USER} -D {OB_DB} {OB_ARGS}"
CSV_FILE_PATH="/data/2/xiongyiqin.xyq/workspace/oceanbase/tools/deploy/data_to_load.csv"
# 定义sql语句
sql_create_table = """
drop table if exists tinghua;
    create table tinghua (
    my_key BIGINT NOT NULL,
    # RAW
    raw_bigint BIGINT NOT NULL,
    raw_int INTEGER NOT NULL,
    raw_char char(20) NOT NULL,
    raw_varchar varchar(40) NOT NULL,
    # DICT
    d_bigint BIGINT NOT NULL,
    d_int INTEGER NOT NULL,
    d_char char(20) NOT NULL,
    d_varchar varchar(40) NOT NULL,
    # RLE
    r_bigint BIGINT NOT NULL,
    r_int INTEGER NOT NULL,
    r_char char(20) NOT NULL,
    r_varchar varchar(40) NOT NULL,
    # CONST
    c_bigint BIGINT NOT NULL,
    c_int INTEGER NOT NULL,
    c_char char(20) NOT NULL,
    c_varchar varchar(40) NOT NULL,
    # INT_BASE_DIFF
    i_bigint BIGINT NOT NULL,
    i_int INTEGER NOT NULL,
    primary key(my_key))row_format = COMPRESSED;
"""


# 建表
subprocess.run(f'echo "{sql_create_table}" | {OB_CLIENT_CONN}', shell=True)
print("[1/3] drop and create table finished!")

# 导入数据
import_sql = f"LOAD DATA /*+ parallel(16) */ infile '{CSV_FILE_PATH}' into table tinghua fields terminated by ',';"

subprocess.run(f'echo "{import_sql}" | {OB_CLIENT_CONN}', shell=True)
print("[2/3] load data from csv finished!")

# 手动major compaction
compaction_sql = """
alter system major freeze;
"""
subprocess.run(f'echo "{compaction_sql}" | {OB_CLIENT_CONN}', shell=True)
print("[3/3] send major freeze task finished!")
