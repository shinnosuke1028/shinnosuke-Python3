# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/9 18:43
# @File: main_re.py
# @Usage: RE正则

import pandas as pd
import re

# Self Repo
# from src.func_demo.func_f import date_f
from src.conf.ftp_conf_bak import *
from src.ftp_wc import ftp_connect, ftp_nlst


data_input_path = './data_output/20191231_FileList.csv'

data = pd.read_csv(data_input_path)

print(data)

y = date_f()[3]['year']
m = date_f()[3]['month']
d = date_f()[3]['day']
h = date_f()[3]['hour']

######
# 非正则方式寻找字段的起止索引号，并捞取
# start = re_rule_tmp.find(re_locate)
# end = re_rule_tmp.rfind(re_locate_2)
# print(f're_rule_tmp[start]: {re_rule_tmp[start]}')
#
# print(f'start: {start}, end: {end}')
# if start != -1:
#     print(re_rule_tmp[start:end])

# a = re.findall(rf'{y}{m}{d}', re_rule_tmp)

######
# # 数据源配置
# data_input = data['LOCAL']
# # 正则规则
# re_rule = f'.*.{y}{m}{d}.*.csv$'
# re_rule2 = f'{y}{m}{d}.*.csv$'
# print(f'\nre_rule:\t{re_rule}\nre_rule2:\t{re_rule2}\n')
#
# match_result = []
# for rs in data_input:
#     if re.match(re_rule, rs):
#         match_result.append(rs)
#         print(f'File match successfully. Filename: {rs}')
#     else:
#         # print(f'Match failed. Filename: {rs}')
#         print(f'Match failed.')
# print(f'\nmatch_result: {match_result}')
#
# match_result2 = [rs for rs in data_input if re.match(re_rule2, rs)]
# print(f'\nmatch_result2: {match_result2}\n\n')


######
# 远程连接Local FTP（浪潮PC）
ip = '192.168.8.100'
remote_path = f'/Mail/'
re_pattern=f'.*{y}{m}{d}.*?csv$'
ftp_local1 = ftp_connect(host=ip, timeout=5)
match_result3 = ftp_nlst(ftp=ftp_local1, remote_path=remote_path, re_pattern=re_pattern)
print(match_result3, '\n')

# HW_PM_3G（华为本地连接不上）
# ip = '172.16.198.14'
# remote_path = f'/export/home/omc/var/fileint/pmneexport/neexport_{y}{m}{d}/'
# re_pattern=f'.*?BTL.*$'
# ftp_local2 = ftp_connect(host=ip, timeout=10)
# if ftp_local2:
#     print('Error.')
# else:
#     match_result4 = ftp_nlst(ftp=ftp_local2, remote_path=remote_path, re_pattern=re_pattern)
#     print(match_result4)

ip = '192.168.62.103'
remote_path = f'/data/py3/project/ftp_monitor/src/logs/'
re_pattern=f'.*{y}{m}{d}.*?log$'
ftp_local2 = ftp_connect(host=ip, timeout=10)
if ftp_local2:
    print('Error.')
else:
    match_result4 = ftp_nlst(ftp=ftp_local2, remote_path=remote_path, re_pattern=re_pattern)
    print(match_result4)