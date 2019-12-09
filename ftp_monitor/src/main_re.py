# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/9 18:43
# @File: main_re.py
# @Usage: RE正则

import pandas as pd
from pandas import DataFrame
import re

# Self Repo
# from src.func_demo.func_f import date_f
from src.conf.ftp_conf_bak import *

data_input_path = './data_output/20191209_FileList.csv'

line = pd.read_csv(data_input_path)
# print(f'line: {line}')

print(type(line))
print(line[line['LOCAL'] == '2019120623_C.csv'])

# with open(data_input_path, newline='', encoding="utf-8") as MyFile:
# # data_df = pd.read_csv(data_input_path)
#     line = MyFile.read()
#     print(line)

y = date_f()[3]['year']
m = date_f()[3]['month']
d = date_f(-2)[3]['day']
h = date_f()[3]['hour']

# start = re_string_tmp.find(re_locate)
# end = re_string_tmp.rfind(re_locate_2)
# print(f're_string_tmp[start]: {re_string_tmp[start]}')
#
# print(f'start: {start}, end: {end}')
#
# if start != -1:
#     print(re_string_tmp[start:end])


# a = re.findall(rf'{y}{m}{d}', re_string_tmp)




re_string = f'{y}{m}{d}'+'.*.csv$'
print(f're_string: {re_string}')
if re.match(re_string, '20191207_SCHEDULER.csv'):
    print('ok')
else:
    print('failed')

