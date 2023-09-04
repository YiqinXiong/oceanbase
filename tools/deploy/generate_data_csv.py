import subprocess
import random
import string
import csv

# 定义 CSV 文件的字段和数据
fields = ["my_key",
    "raw_bigint", "raw_int", "raw_char", "raw_varchar",
    "d_bigint", "d_int", "d_char", "d_varchar", 
    "r_bigint", "r_int", "r_char", "r_varchar", 
    "c_bigint", "c_int", "c_char", "c_varchar", 
    "i_bigint", "i_int"]
my_rows = []

def write_to_csv(header, rows):
    # 打开 CSV 文件并写入数据
    with open('data_to_load.csv', 'w', newline='') as csv_file:
        # 创建 CSV writer 对象
        writer = csv.writer(csv_file)
        # 写入表头
        # writer.writerow(fields)
        # 写入数据行
        writer.writerows(rows)

# 插入数据
ROW_COUNT=1000000
DICT_N=10
RLE_N=10
CONST_N=10
RLE_DUPLICATE_N=10

MARK_0=0.05
MARK_1=0.25
MARK_2=0.5
MARK_3=0.75

def get_random_string(length):
    """
    生成随机的长度为length的字符串，包含空格、大小写字母和数字
    """
    # 定义字符串的取值范围（包括空格、大小写字母和数字）
    letters = string.ascii_letters + string.digits + '-'
    # 使用 random.choices() 方法从字符串的取值范围中随机选择字符，生成一个长度为length的列表
    random_list = random.choices(letters, k=length)
    # 将列表中的字符连接成字符串，返回生成的随机字符串
    rand_str = ''.join(random_list)
    return rand_str

def get_random_bool():
    """
    有固定99%的概率返回True，固定10%的概率返回False
    """
    random_num = random.random()  # 生成一个0到1之间的随机数
    if random_num < 0.99:
        return True
    else:
        return False


bigint_dict = [100000 * x for x in range(DICT_N)]
int_dict = [1000 * x for x in range(DICT_N)]
char_dict = [get_random_string(8) for x in range(DICT_N)]
varchar_dict = [get_random_string(random.randint(3, 33)) for x in range(DICT_N)]
bigint_rle = [100000 * x for x in range(RLE_N)]
int_rle = [1000 * x for x in range(RLE_N)]
char_rle = [get_random_string(8) for x in range(RLE_N)]
varchar_rle = [get_random_string(random.randint(3, 33)) for x in range(RLE_N)]

def get_row(row_id=0):
    my_key = row_id
    # RAW
    raw_bigint = random.randint(-1e18, 1e18)
    raw_int = random.randint(-1e9, 1e9)
    raw_char = get_random_string(8)
    raw_varchar = get_random_string(random.randint(3, 33))
    # DICT
    d_bigint = -2000000000 + bigint_dict[i % DICT_N]
    d_int = 500 + int_dict[i % DICT_N]
    d_char = char_dict[i % DICT_N]
    d_varchar = varchar_dict[i % DICT_N]
    # RLE
    block_id = row_id // RLE_DUPLICATE_N
    r_bigint = 10000000 + bigint_rle[block_id % RLE_N]
    r_int = -5 + int_rle[block_id % RLE_N]
    r_char = char_rle[block_id % RLE_N]
    r_varchar = varchar_rle[block_id % RLE_N]
    # CONST
    c_bigint = 11111111
    c_int = 2222
    c_char = 'lth-1234'
    c_varchar = 'tinghua-22223333-你好——我是庭华'
    is_const = get_random_bool()
    if not is_const:
        c_bigint = -2000000000 + i % DICT_N
        c_int = 500 + i % DICT_N
        c_char = char_dict[i % DICT_N]
        c_varchar = varchar_dict[i % DICT_N]
    # INT_BASE_DIFF  
    i_bigint = 1000000000 + random.randint(-5000, 5000)
    i_int = -300 + random.randint(-5000, 5000)

    return [my_key,
    raw_bigint, raw_int, raw_char, raw_varchar, 
    d_bigint, d_int, d_char, d_varchar, 
    r_bigint, r_int, r_char, r_varchar, 
    c_bigint, c_int, c_char, c_varchar, 
    i_bigint, i_int]



for i in range(ROW_COUNT):
    if i >= MARK_3*ROW_COUNT:
        print(f"finished {i} rows")
        MARK_3 = 1
    elif i >= MARK_2*ROW_COUNT:
        print(f"finished {i} rows")
        MARK_2 = 1
    elif i >= MARK_1*ROW_COUNT:
        print(f"finished {i} rows")
        MARK_1 = 1
    elif i >= MARK_0*ROW_COUNT:
        print(f"finished {i} rows")
        MARK_0 = 1

    my_rows.append(get_row(row_id = i))

write_to_csv(fields, my_rows)
