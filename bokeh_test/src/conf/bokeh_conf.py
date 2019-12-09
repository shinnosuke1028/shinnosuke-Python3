# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/12/2 17:29
# @File: bokeh_conf.py

"""
Bokeh输出配置
"""
from time import ctime
from src.func_demo.func_f import date_f

# usecols
cols = ['DATA_STYLE', 'HOUR', 'NORMAL_FILE_SIZE/MB', 'NOW_SIZE/MB']
# cols_rename
cols_rename_dic = {'DATA_STYLE': 'STYLE', 'NORMAL_FILE_SIZE/MB': 'NORMAL', 'NOW_SIZE/MB': 'NOW'}

html_output_path = './data_output/' + date_f()[0] + '_log_lines.html'

# print(ctime())
print(html_output_path)
