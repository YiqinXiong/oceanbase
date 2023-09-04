import subprocess
import random
import string
import csv
import math
import os
import time
import pickle
import concurrent.futures
import multiprocessing
from tqdm import tqdm
from decimal import Decimal
import argparse

# 创建解析器
parser = argparse.ArgumentParser(description='My script')

# 添加参数
parser.add_argument('--num', type=int, help='Input file path')
parser.add_argument('--dict_n', type=int, help='dict length of DICT')
parser.add_argument('--rle_n', type=int, help='dict length of RLE')
parser.add_argument('--const_n', type=int, help='dict length of CONST')
parser.add_argument('--mode', type=str, required=True ,choices=['cache', 'csv', 'load', 'run', 'sql'], help=
'''
    cache: only generate cache;
    data; csv: dump to csv;
    load: load csv to database;
    run: run test sql query;
    sql: generate test sql query;
''')

# 解析命令行参数
global_args = parser.parse_args()


# 路径参数
RANDOM_CACHE_DATA_PATH="./random_cache_data.pkl"
CSV_OUTPUT_DIR="/data/2/xiongyiqin.xyq/workspace/oceanbase/tools/deploy/csv_data_to_load"
MYSQL_TEST_OUTPUT_DIR="/data/2/xiongyiqin.xyq/workspace/oceanbase/tools/deploy/mysql_test/test_suite/white_filter"
RUN_SQL_RESULT_OUTPUT_DIR="/data/2/xiongyiqin.xyq/workspace/oceanbase/tools/deploy/xyq_test_res/test"

# 数量参数
ROW_COUNT = 10000000 if not global_args.num else global_args.num
DICT_N = 10 if not global_args.dict_n else global_args.dict_n
RLE_N = 10 if not global_args.rle_n else global_args.rle_n
CONST_N = 10 if not global_args.const_n else global_args.const_n

# INT_BASE_DIFF的BASE
INT_BASE_DIFF_BIGINT=-1e18
INT_BASE_DIFF_INT=-1e9
INT_BASE_DIFF_UBIGINT=1e18
INT_BASE_DIFF_UINT=1e9

# 数据长度/精度参数
CHAR_LEN=8
VARCHAR_LEN_MIN=3
VARCHAR_LEN_MAX=33
DECIMAL_PREC=11
DECIMAL_SCALE=3

# obclient参数
OB_CLIENT_PATH="/data/2/xiongyiqin.xyq/workspace/oceanbase/deps/3rd/u01/obclient/bin/obclient"
OB_IP="127.0.0.1"
OB_PORT="43700"
# OB_USER="root@mytpch"
OB_USER="root@mysql"
OB_DB="xyq"
OB_ARGS="-f -c -A -vvv"
OB_CLIENT_CONN=f"{OB_CLIENT_PATH} -h {OB_IP} -P {OB_PORT} -u {OB_USER} -D {OB_DB} {OB_ARGS}"
OB_CLIENT_CONN_NO_DB=f"{OB_CLIENT_PATH} -h {OB_IP} -P {OB_PORT} -u {OB_USER} {OB_ARGS}"

# 全局变量
encoding_prefix_map = {
    'RAW': "raw_",
    'DICT': "d_",
    'RLE': "r_",
    'CONST': "c_",
    'INT_BASE_DIFF': "i_",
}

data_type_sql_map = {
    'bigint': 'BIGINT',
    'int': 'INTEGER',
    'char': f'char({20 if CHAR_LEN < 20 else CHAR_LEN+10})',
    'varchar': f'varchar({40 if VARCHAR_LEN_MAX < 40 else VARCHAR_LEN_MAX+10})',
    'decimal': f'decimal({DECIMAL_PREC},{DECIMAL_SCALE})',
    'ubigint': 'BIGINT UNSIGNED',
    'uint': 'INTEGER UNSIGNED',
}
data_type_sql_map_int_base = {
    'bigint': 'BIGINT',
    'int': 'INTEGER',
    'ubigint': 'BIGINT UNSIGNED',
    'uint': 'INTEGER UNSIGNED',
}

def round_to_nearest_10_power(x):
    """
    将 x 取整到最接近的 10 的次幂的整数
    """
    power = math.floor(math.log10(x))
    return round(x/10**power) * 10**power

def write_to_csv(fname, header, rows):
    # 打开 CSV 文件并写入数据
    with open(fname, 'w', newline='') as csv_file:
        # 创建 CSV writer 对象
        writer = csv.writer(csv_file)
        # 写入表头
        # writer.writerow(header)
        # 写入数据行
        writer.writerows(rows)

def get_random_int(seed, l_border, r_border):
    """
    生成随机的数字, 从l_border到r_border
    """
    random.seed(seed)
    return random.randint(l_border, r_border)

def get_random_char(seed, length):
    """
    生成随机的长度为length的定长字符串, 包含大小写字母, 数字 和 '-'
    """
    # 随机串范围
    letters = string.ascii_letters + string.digits + '-'
    # 使用 random.choices() 方法从字符串的取值范围中随机选择字符，生成一个长度为length的列表
    random.seed(seed)
    random_list = random.choices(letters, k=length)
    # 将列表中的字符连接成字符串，返回生成的随机字符串
    rand_str = ''.join(random_list)
    return rand_str

def get_random_varchar(seed, min_len, max_len):
    """
    生成随机的长度为min_len到max_len的变长字符串, 包含大小写字母, 数字 和 '-'
    """
    assert(min_len <= max_len)
    return get_random_char(seed, length=get_random_int(seed=seed, l_border=min_len, r_border=max_len))

def get_random_decimal(seed, precision, scale):
    """
    生成随机的 Decimal 数字，精度为 precision, 小数点后 scale 位
    """
    assert(precision > scale)
    assert(scale > 0)
    random.seed(seed)
    integer_part_border = 10 ** (precision - scale) - 1
    integer_part = random.randint(-integer_part_border, integer_part_border)
    random.seed(seed+1)
    decimal_part = random.randint(0, 10 ** scale - 1)
    return Decimal(f"{integer_part}.{decimal_part:0>{scale}}")

def get_random_bool(seed, p):
    """
    有固定p的概率返回True, 固定1-p的概率返回False
    """
    assert(p<=1 and p>=0)
    random.seed(seed)
    random_num = random.random()  # 生成一个0到1之间的随机数
    if random_num < p:
        return True
    else:
        return False

get_random_functions = {
    "int": get_random_int,
    "char": get_random_char,
    "varchar": get_random_varchar,
    "decimal": get_random_decimal,
}

# insert进程worker
def worker_insert(data_type, seed_begin, seed_end, arg_tuple):
    res = {}
    res['data_type'] = data_type
    res['arg_tuple'] = str(arg_tuple)
    res['value_list'] = []
    for s in tqdm(range(seed_begin, seed_end+1), desc=f"Random {data_type:>8} {str(arg_tuple):>20}"):
        k = get_random_functions[data_type](s, *arg_tuple)
        res['value_list'].append(k)
    return res

class RandomCache:
    def __init__(self):
        self.data = {
            "int": {},
            "char": {},
            "varchar": {},
            "decimal": {},
        }
        self.data_path = RANDOM_CACHE_DATA_PATH

    def insert_val(self, seed):
        data_types = ['int', 'int', 'char', 'varchar', 'decimal', 'int', 'int', 'int', 'int', 'int', 'int']
        args_list = [(-1e18, 1e18), (-1e9, 1e9), (CHAR_LEN, ), (VARCHAR_LEN_MIN, VARCHAR_LEN_MAX), (DECIMAL_PREC, DECIMAL_SCALE), (1, 2e18), (1, 2e9), (-5000, 5000), (-5000, 5000), (1000001, 1005000), (10001, 15000)]
        assert(len(data_types)==len(args_list))
        need_num = seed * 2
        # 筛选出需要生成随机数的任务
        task_data_types = []
        task_args_list = []
        task_len_value_list = []
        for idx, data_type in enumerate(data_types):
            arg_tuple = args_list[idx]
            if data_type not in self.data:
                self.data[data_type] = {}
            arg_str = str(arg_tuple)
            # print(f"args_tuple {args_tuple} type(args_tuple) {type(args_tuple)} *args {print(*(args_tuple))}")
            # 如果不存在此参数类型的value_list, 则创建
            if arg_str not in self.data[data_type]:
                self.data[data_type][arg_str] = []
            value_list = self.data[data_type][arg_str]
            if need_num >= len(value_list):
                task_data_types.append(data_type)
                task_args_list.append(arg_tuple)
                task_len_value_list.append(len(value_list))
        assert(len(task_args_list)==len(task_data_types) and len(task_len_value_list)==len(task_args_list))
        len_types = len(task_args_list)
        if len_types == 0:
            print("no need to generate cache data")
            return
        # 并行执行
        with concurrent.futures.ProcessPoolExecutor(max_workers=len_types) as executor:
            # 提交任务到进程池
            try:
                multiprocesses = executor.map(worker_insert, task_data_types, task_len_value_list, [need_num]*len_types, task_args_list)
                for function_return_value in multiprocesses:
                    self.data[function_return_value['data_type']][function_return_value['arg_tuple']] += function_return_value['value_list']
            except ValueError as e:
                print(e)
        self.save_cache_to_disk()

    def get_val(self, data_type, seed, *args):
        if data_type not in self.data:
            print(f"data_type {data_type} not in self.data {self.data}")
            return None
        # 如果不存在此参数类型的value_list, 则报错退出
        arg_str = str(args)
        if arg_str not in self.data[data_type]:
            print(f'arg_str not in data["{data_type}"], arg_str {arg_str}, self.data["{data_type}"].keys() {self.data[data_type].keys()} ,please regenerate cache')
            return None
        # 如果seed的值不小于value_list的长度, 则报错退出
        value_list = self.data[data_type][arg_str]
        if seed >= len(value_list):
            print(f'seed {seed} exceed value_list range {len(value_list)}, seed {seed}, please regenerate cache')
            return None
        # 返回cache值
        return value_list[seed]
    
    def save_cache_to_disk(self):
        # 将对象保存到文件中
        try:
            with open(self.data_path, 'wb') as f:
                pickle.dump(self.data, f)
        except IOError as e:
            print(e)
    
    def load_cache_from_disk(self, path=''):
        if not path or path == '':
            path = self.data_path
        # 从文件中加载对象
        try:
            with open(path, 'rb') as f:
                self.data = pickle.load(f)
        except FileNotFoundError as e:
            print(e)


# 生成一行普通行
def get_row_values(s, rc, char_len, varchar_len_min, varchar_len_max, decimal_precision, decimal_scale):
    col_bigint = rc.get_val('int', s, -1e18, 1e18)
    col_int = rc.get_val('int', s, -1e9, 1e9)
    col_char = rc.get_val('char', s, char_len)
    col_varchar = rc.get_val('varchar', s, varchar_len_min, varchar_len_max)
    col_decimal = rc.get_val('decimal', s, decimal_precision, decimal_scale)
    col_ubigint = rc.get_val('int', s, 1, 2e18)
    col_uint = rc.get_val('int', s, 1, 2e9)
    row_values = [col_bigint, col_int, col_char, col_varchar, col_decimal, col_ubigint, col_uint]
    return row_values

# 生成一行INT_BASE_DIFF行
def get_int_row_values(s, rc):
    col_bigint = rc.get_val('int', s, -5000, 5000)
    col_int = rc.get_val('int', s, -5000, 5000)
    col_ubigint = rc.get_val('int', s, 1000001, 1005000)
    col_uint = rc.get_val('int', s, 10001, 15000)
    int_row_values = [col_bigint, col_int, col_ubigint, col_uint]
    return int_row_values

def get_row(row_id, encode_type, random_cache):
    # 参数检查
    if encode_type not in encoding_prefix_map:
        print("INVALID encode_type:", encode_type)
        return []
    # 生成row
    row_key = row_id
    seed = 0
    row_values = []
    if encode_type == 'INT_BASE_DIFF':
        seed = row_id
        row_values = get_int_row_values(seed, random_cache)
    else:
        if encode_type == 'RAW':
            seed = row_id
        elif encode_type == 'DICT':
            seed = row_id % DICT_N
        elif encode_type == 'RLE':
            rle_duplicate_n = round_to_nearest_10_power(1000 / RLE_N)
            seed = (row_id // rle_duplicate_n) % RLE_N
        elif encode_type == 'CONST':
            seed = 0
            is_const = get_random_bool(seed=row_id, p=0.999)
            if not is_const:
                seed = 1 + (row_id % CONST_N)
        row_values = get_row_values(seed, random_cache, char_len=CHAR_LEN, 
                                    varchar_len_min=VARCHAR_LEN_MIN, varchar_len_max=VARCHAR_LEN_MAX, 
                                    decimal_precision=DECIMAL_PREC, decimal_scale=DECIMAL_SCALE)
    return [row_key] + row_values

# dump进程worker
def worker_dump(encode_type, fname, header, random_cache):
    my_rows = []
    data_range = range(ROW_COUNT)
    for row_id in tqdm(data_range, desc=f"Dump CSV {encode_type:>14}"):
        # 遍历的具体操作
        my_rows.append(get_row(row_id, encode_type, random_cache))
    write_to_csv(fname, header, rows=my_rows)

# 构造load data相关的sql语句
def generate_load_data_sql(encode_type):
    prefix = encoding_prefix_map[encode_type]
    table_name = f"{prefix}test"
    # 建表语句
    sql_drop_and_create_table = f'drop table if exists {table_name};\n'
    sql_drop_and_create_table += f'create table {table_name} (\n'
    sql_drop_and_create_table += f'\t{prefix}key BIGINT NOT NULL,\n'
    if encode_type == 'INT_BASE_DIFF':
        for col_type in data_type_sql_map_int_base.keys():
            col_name = f'{prefix}{col_type}'
            sql_drop_and_create_table += f'\t{col_name} {data_type_sql_map_int_base[col_type]},\n'
    else:
        for col_type in data_type_sql_map.keys():
            col_name = f'{prefix}{col_type}'
            sql_drop_and_create_table += f'\t{col_name} {data_type_sql_map[col_type]},\n'
    sql_drop_and_create_table += f'\tprimary key({prefix}key))row_format = COMPRESSED;'
    # 旁路导入语句
    file_name = f"{CSV_OUTPUT_DIR}/{encode_type}.csv"
    if not os.path.exists(file_name):
        print(f'File {file_name} does not exist!')
        res = f'{sql_drop_and_create_table}\n'
        return res
    sql_load_data = f"LOAD DATA /*+ parallel({multiprocessing.cpu_count()}) */ infile '{CSV_OUTPUT_DIR}/{encode_type}.csv' into table {table_name} fields terminated by ',';"
    # 拼接最终sql
    res = f'{sql_drop_and_create_table}\n\n{sql_load_data}\n'
    return res

################# MODE FUNCTIONS BEGIN #################
def cache_mode():
    # 1. 从磁盘导入cache
    random_cache = RandomCache()
    print("1. loading cache from disk...")
    random_cache.load_cache_from_disk(path=RANDOM_CACHE_DATA_PATH)
    # 2. 生成/补全cache数据
    print("2. generating cache data...")
    random_cache.insert_val(ROW_COUNT)

def csv_mode():
    # 多线程从cache读取数据并导出到CSV文件
    def dump_to_csv(random_cache, multi_processing=False):
        encode_types = list(encoding_prefix_map.keys())
        fnames = [f"{CSV_OUTPUT_DIR}/{encode_type}.csv" for encode_type in encode_types]
        headers = [[f"{encoding_prefix_map[encode_type]}key"] \
                        + [f"{encoding_prefix_map[encode_type]}{data_type}" for data_type in data_type_sql_map.keys()] \
                            for encode_type in encode_types]
        if (multi_processing):
            # 创建进程池并指定进程数量
            with concurrent.futures.ProcessPoolExecutor() as executor:
                # 提交任务到进程池
                try:
                    executor.map(worker_dump, encode_types, fnames, headers, [random_cache]*len(encode_types))
                except ValueError as e:
                    print(e)
                except Exception as e:
                    print(e)
        else:
            for i in range(len(encode_types)):
                try:
                    worker_dump(encode_types[i], fnames[i], headers[i], random_cache);
                except ValueError as e:
                    print(e)
                except Exception as e:
                    print(e)
                
    
    # 0. 建立目录
    if not os.path.exists(CSV_OUTPUT_DIR):
        os.makedirs(CSV_OUTPUT_DIR)
    # 1. 从磁盘导入cache
    random_cache = RandomCache()
    print("1. loading cache from disk...")
    random_cache.load_cache_from_disk(path=RANDOM_CACHE_DATA_PATH)
    # 2. 生成/补全cache数据
    print("2. generating cache data...")
    random_cache.insert_val(ROW_COUNT)
    # 3. 多线程生成数据, 每个线程负责一种编码
    print("3. dump data to csv...")
    print(f"\tGenerating data into '{CSV_OUTPUT_DIR}'")
    dump_to_csv(random_cache, multi_processing=False)

def load_mode():
    # 建库
    sql_create_database = f'create database if not exists {OB_DB};'
    subprocess.run(f'echo "{sql_create_database}" | {OB_CLIENT_CONN_NO_DB}', shell=True)
    # 主体逻辑
    print(f"LOAD DATA from csv files in '{CSV_OUTPUT_DIR}'")
    encode_types = list(encoding_prefix_map.keys())
    type_range = range(len(encode_types))
    for idx in tqdm(type_range, desc=f"LOAD DATA from csv"):
        encode_type = encode_types[idx]
        sql_load_csv = generate_load_data_sql(encode_type)
        subprocess.run(f'echo "{sql_load_csv}" | {OB_CLIENT_CONN}', shell=True)
    # major compaction语句
    sql_major_freeze = 'alter system major freeze;'
    subprocess.run(f'echo "{sql_major_freeze}" | {OB_CLIENT_CONN}', shell=True)
    print("Data loaded and send major freeze to observer.")

def sql_mode(run=False):
    # in范围: 一般:不存在/一部分在 CONST:不存在/只有CONST存在/只有部分非CONST的值存在
    # in数组长度: 4/40/400
    encodings = ["RAW", "DICT", "RLE", "CONST", "INT_BASE_DIFF"]
    # encodings = ["RAW"]
    param_nums = [4, 40, 400]

    # 0. 建立目录
    if not run and not os.path.exists(MYSQL_TEST_OUTPUT_DIR):
        os.makedirs(MYSQL_TEST_OUTPUT_DIR)
        os.makedirs(MYSQL_TEST_OUTPUT_DIR+"/t")
        os.makedirs(MYSQL_TEST_OUTPUT_DIR+"/r")
    # 1. 从磁盘导入cache
    random_cache = RandomCache()
    print("1. loading cache from disk...")
    random_cache.load_cache_from_disk(path=RANDOM_CACHE_DATA_PATH)
    # 2. 生成/补全cache数据
    print("2. generating cache data...")
    random_cache.insert_val(ROW_COUNT)
    # 3. 从cache生成数据
    def get_col_no_list(encode_type, col_type, num):
        # 参数检查
        if encode_type not in encoding_prefix_map:
            print("INVALID encode_type:", encode_type)
            return []
        if encode_type == 'INT_BASE_DIFF':
            if col_type == 'bigint':
                return [get_random_int(s, -100000, -5000) for s in range(ROW_COUNT, ROW_COUNT+num)]
            elif col_type == 'int':
                return [get_random_int(s, -100000, -5000) for s in range(ROW_COUNT, ROW_COUNT+num)]
            elif col_type == 'ubigint':
                return [get_random_int(s, 10000, 1000001) for s in range(ROW_COUNT, ROW_COUNT+num)]
            elif col_type == 'uint':
                return [get_random_int(s, 1, 10001) for s in range(ROW_COUNT, ROW_COUNT+num)]
            else:
                print("INVALID col_type:", col_type)
                return []
        else:
            if col_type == 'bigint':
                return [get_random_int(s, -1-2e18, -1-1e18) for s in range(ROW_COUNT, ROW_COUNT+num)]
            elif col_type == 'int':
                return [get_random_int(s, -1-2e9, -1-1e9) for s in range(ROW_COUNT, ROW_COUNT+num)]
            elif col_type == 'char':
                return [("##" + get_random_char(s, CHAR_LEN-2)) for s in range(ROW_COUNT, ROW_COUNT+num)]
            elif col_type == 'varchar':
                return [("##" + get_random_varchar(s, VARCHAR_LEN_MIN, VARCHAR_LEN_MAX)) for s in range(ROW_COUNT, ROW_COUNT+num)]
            elif col_type == 'decimal':
                return [get_random_decimal(s, DECIMAL_PREC, DECIMAL_SCALE) for s in range(ROW_COUNT, ROW_COUNT+num)]
            elif col_type == 'ubigint':
                return [get_random_int(s, 1+1e18, 1+2e18) for s in range(ROW_COUNT, ROW_COUNT+num)]
            elif col_type == 'uint':
                return [get_random_int(s, 1+1e9, 1+2e9) for s in range(ROW_COUNT, ROW_COUNT+num)]
            else:
                print("INVALID col_type:", col_type)
                return []
    
    def get_col_in_list(encode_type, col_type, random_cache, num):
        res = []
        if encode_type == "INT_BASE_DIFF":
            col_type_2_col_id = {
                'bigint': 0,
                'int': 1,
                'ubigint': 2,
                'uint': 3,
            }
        else:
            col_type_2_col_id = {
                'bigint': 0,
                'int': 1,
                'char': 2,
                'varchar': 3,
                'decimal': 4,
                'ubigint': 5,
                'uint': 6,
            }
        col_id = col_type_2_col_id[col_type]
        
        if encode_type == 'DICT':
            num = min(num, DICT_N)
        elif encode_type == 'RLE':
            num = min(num, RLE_N)
        elif encode_type == 'CONST':
            num = min(num, CONST_N)
        for row_id in range(num):
            if encode_type == 'INT_BASE_DIFF':
                seed = row_id
                row_values = get_int_row_values(seed, random_cache)
            else:
                if encode_type == 'RAW':
                    seed = row_id
                elif encode_type == 'DICT':
                    seed = row_id % DICT_N
                elif encode_type == 'RLE':
                    seed = row_id % RLE_N
                elif encode_type == 'CONST':
                    seed = 1 + (row_id % CONST_N)
                row_values = get_row_values(seed, random_cache, char_len=CHAR_LEN, 
                                            varchar_len_min=VARCHAR_LEN_MIN, varchar_len_max=VARCHAR_LEN_MAX, 
                                            decimal_precision=DECIMAL_PREC, decimal_scale=DECIMAL_SCALE)

            res.append(row_values[col_id])
        return res
    
    def get_const_val(col_type, random_cache):
        col_type_2_col_id = {
            'bigint': 1,
            'int': 2,
            'char': 3,
            'varchar': 4,
            'decimal': 5,
            'ubigint': 6,
            'uint': 7,
        }
        const_seed = 0
        row_values = get_row_values(const_seed, random_cache, char_len=CHAR_LEN, 
                                    varchar_len_min=VARCHAR_LEN_MIN, varchar_len_max=VARCHAR_LEN_MAX, 
                                    decimal_precision=DECIMAL_PREC, decimal_scale=DECIMAL_SCALE)
        return [row_values[col_type_2_col_id[col_type]-1]]

    RAW_NO = {col_t:get_col_no_list('RAW', col_t, max(param_nums)) for col_t in data_type_sql_map.keys()}
    RAW_ALL = {col_t:get_col_in_list('RAW', col_t, random_cache, max(param_nums)) for col_t in data_type_sql_map.keys()}
    RAW_PART = {col_t:RAW_ALL[col_t]+RAW_NO[col_t] for col_t in data_type_sql_map.keys()}

    DICT_NO = {col_t:get_col_no_list('DICT', col_t, max(param_nums)) for col_t in data_type_sql_map.keys()}
    DICT_ALL = {col_t:get_col_in_list('DICT', col_t, random_cache, max(param_nums)) for col_t in data_type_sql_map.keys()}
    DICT_PART = {col_t:DICT_ALL[col_t]+DICT_NO[col_t] for col_t in data_type_sql_map.keys()}

    RLE_NO = {col_t:get_col_no_list('RLE', col_t, max(param_nums)) for col_t in data_type_sql_map.keys()}
    RLE_ALL = {col_t:get_col_in_list('RLE', col_t, random_cache, max(param_nums)) for col_t in data_type_sql_map.keys()}
    RLE_PART = {col_t:RLE_ALL[col_t]+RLE_NO[col_t] for col_t in data_type_sql_map.keys()}

    CONST_NO = {col_t:get_col_no_list('CONST', col_t, max(param_nums)) for col_t in data_type_sql_map.keys()}
    CONST_ALL = {col_t:get_col_in_list('CONST', col_t, random_cache, max(param_nums)) for col_t in data_type_sql_map.keys()}
    CONST_PART = {col_t:CONST_ALL[col_t]+CONST_NO[col_t] for col_t in data_type_sql_map.keys()}
    CONST_PART_CONST_IN = {col_t:get_const_val(col_t, random_cache)+CONST_NO[col_t] for col_t in data_type_sql_map.keys()}

    INT_BASE_DIFF_NO = {col_t:get_col_no_list('INT_BASE_DIFF', col_t, max(param_nums)) for col_t in data_type_sql_map_int_base.keys()}
    INT_BASE_DIFF_ALL = {col_t:get_col_in_list('INT_BASE_DIFF', col_t, random_cache, max(param_nums)) for col_t in data_type_sql_map_int_base.keys()}
    INT_BASE_DIFF_PART = {col_t:INT_BASE_DIFF_ALL[col_t]+INT_BASE_DIFF_NO[col_t] for col_t in data_type_sql_map_int_base.keys()}
    
    encoding_case_map = {
        'RAW': {
            'NO':RAW_NO,
            'PART':RAW_PART,
        },
        'DICT': {
            'NO': DICT_NO, 
            'PART': DICT_PART,
        },
        'RLE': {
            'NO': RLE_NO, 
            'PART': RLE_PART,
        },
        'CONST': {
            'NO': CONST_NO,
            'PART': CONST_PART,
            'PART_CONST_IN': CONST_PART_CONST_IN,
        },
        'INT_BASE_DIFF': {
            'NO': INT_BASE_DIFF_NO,
            'PART': INT_BASE_DIFF_PART,
        },
    }

    encoding_value_num_map = {
        'RAW': len(RAW_ALL['int']),
        'DICT': len(DICT_ALL['int']),
        'RLE': len(RLE_ALL['int']),
        'CONST': len(CONST_ALL['int']),
        'INT_BASE_DIFF': len(INT_BASE_DIFF_ALL['int']),
    }

    def get_sql_from_map(encode_type='RAW', params_num=200):
        mysql_test_content = ""
        case_list = encoding_case_map[encode_type]
        prefix = encoding_prefix_map[encode_type]

        for case_name, each_case in case_list.items():
            mysql_test_content += f"\n--echo ==== case name = {case_name} ====\n"
            for d_type,v_list in each_case.items():
                valid_value_num = min(encoding_value_num_map[e]//2, pn//2)
                combined_v_list = v_list[:(valid_value_num)]+v_list[-(params_num-valid_value_num):]
                if d_type == 'decimal':
                    combined_v_list = [round(float(x), DECIMAL_SCALE) for x in combined_v_list]
                query = f"select count({prefix+d_type}) from {prefix}test where {prefix+d_type} in {tuple(combined_v_list)};"
                mysql_test_content += f'{query}\n'
        return mysql_test_content

    def run_sql_from_map(encode_type='RAW', params_num=200, repeat_time=1):
        my_rows = []
        case_list = encoding_case_map[encode_type]
        prefix = encoding_prefix_map[encode_type]

        for case_name, each_case in case_list.items():
            print("\n======== case name =", case_name, "========")
            case_row = []
            for repeat_idx in range(repeat_time+1):
                temp_case_row = []
                for d_type,v_list in each_case.items():
                    valid_value_num = min(encoding_value_num_map[e]//2, pn//2)
                    combined_v_list = v_list[:(valid_value_num)]+v_list[-(params_num-valid_value_num):]
                    if d_type == 'decimal':
                        combined_v_list = [round(float(x), DECIMAL_SCALE) for x in combined_v_list]
                    query = f"select count({prefix+d_type}) from {prefix}test where {prefix+d_type} in {tuple(combined_v_list)};"
                    print(query)
                    cmd = f'echo "{query}" | {OB_CLIENT_CONN} | grep "sec)" '
                    cmd += "| awk '{match($0, /[0-9]+\.[0-9]+/); print substr($0, RSTART, RLENGTH)}'"
                    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    res_str = result.stdout.decode('utf-8')
                    print(res_str)
                    temp_case_row.append(float(res_str.strip()))
                    # 如果命令执行出错，则打印错误信息
                    if result.returncode != 0:
                        print(result.stderr.decode('utf-8'))
                if repeat_idx==0:
                    pass
                elif repeat_idx==1:
                    case_row = temp_case_row
                else:
                    case_row = [a + b for a, b in zip(case_row, temp_case_row)]    
            case_row = [round(x/repeat_time, 3) for x in case_row]
            my_rows.append([case_name, params_num] + case_row)
        return my_rows
    
    if run:
        print("3. run sql...")
        my_rows = []
        for e in encodings:
            print("\n======== encoding =", e, "========")
            my_rows.append(['#'])
            my_rows.append([f'# encoding = {e}'])
            header = ['Scenario', 'num of IN (*)']
            header += list(data_type_sql_map_int_base.keys()) if e=='INT_BASE_DIFF' else list(data_type_sql_map.keys())
            my_rows.append(header)
            for pn in param_nums:
                print("==== params number =", pn, "====")
                my_rows += run_sql_from_map(e, pn, repeat_time=3)
        write_to_csv(fname=f"{RUN_SQL_RESULT_OUTPUT_DIR}/result.csv", header=[], rows=my_rows)
    else:
        print("3. generating sql...")
        for e in encodings:
            prefix = encoding_prefix_map[e]
            table_name = f"{prefix}test"
            print("\n======== encoding =", e, "========")
            fname = f"{MYSQL_TEST_OUTPUT_DIR}/t/in_operator_{e}.test"
            fcontent = ""
            fcontent += "--disable_query_log\n"
            fcontent += "let $char_charset = 'utf8';\n"
            fcontent += '--disable_warnings\n'
            fcontent += 'drop database if exists pushdown_test;\n'
            fcontent += '--enable_warnings\n'
            fcontent += 'create database pushdown_test;\n'
            fcontent += 'use pushdown_test;\n'
            fcontent += "set timestamp = 1600000000;\n"
            fcontent += "set ob_trx_timeout = 10000000000;\n"
            fcontent += "set ob_query_timeout = 10000000000;\n"
            fcontent += "--result_format 4\n"
            # fcontent += "\n--echo Check if need to create table and load data...\n"
            # fcontent += f"let $table_exist = query_get_value(select count(*) as table_cnt from information_schema.tables where table_name='{table_name}', table_cnt, 1);\n"
            
            # fcontent += "if(!$table_exist)\n"
            # fcontent += "{\n"
            fcontent += "--disable_warnings\n"
            fcontent += "--disable_result_log\n"
            fcontent += generate_load_data_sql(e)
            fcontent += "alter system major freeze;\n"
            # fcontent += "let $comp_status =query_get_value(select status from oceanbase.cdb_ob_major_compaction where tenant_id=1, status, 1);\n"
            fcontent += '--source mysql_test/include/wait_daily_merge.inc\n'
            fcontent += "--disable_query_log\n"
            fcontent += "--enable_result_log\n"
            fcontent += "--enable_warnings\n"
            # fcontent += "}\n"
            
            fcontent += "\n--echo Set pushdown level to 2...\n"
            fcontent += "--disable_result_log\n"
            fcontent += 'alter system set _pushdown_storage_level=2;\n'
            fcontent += 'alter system flush plan cache;\n'
            fcontent += "--enable_result_log\n"
            fcontent += '\n--echo Sleep 5 secs and run queries...\n'
            fcontent += 'real_sleep 5;\n'

            # fcontent += 'let $cnt=0;\n'
            # fcontent += 'while($cnt<4)\n'
            # fcontent += '{\n'
            # fcontent += '--echo ======== round = $cnt ========\n'
            fcontent += '--start_timer\n'
            for pn in param_nums:
                print(f'\n==== params_number={pn} ====')
                fcontent += f'\n--echo ====params_number={pn}====\n'
                fcontent += get_sql_from_map(e, pn)
            fcontent += '--end_timer\n'
            # fcontent += '\tinc $cnt;\n'
            # fcontent += '}\n'
            fcontent += '--echo  ######## 删除测试数据库 ##########\n'
            fcontent += '--disable_warnings\n'
            fcontent += 'drop database if exists pushdown_test;\n'
            fcontent += '--enable_warnings\n'
            fcontent += '--enable_query_log\n'

            with open(fname, "w") as f:
                # 将字符串写入文件
                f.write(fcontent)
    
################# MODE FUNCTIONS END #################

if __name__ == "__main__":
    if global_args.mode == 'cache':
        cache_mode()
    elif global_args.mode == 'csv':
        csv_mode()
    elif global_args.mode == 'load':
        load_mode()
    elif global_args.mode == 'run':
        sql_mode(run=True)
    elif global_args.mode == 'sql':
        sql_mode(run=False)
    else:
        print(f"unknown mode {global_args.mode}")
