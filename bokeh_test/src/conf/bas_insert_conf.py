# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/9/19 0:02
# @File: bas_conf.py

"""数据入库 &&入库文件索引配置"""
# 数据库连接信息
# connect_info = 'LRNOP/Inspur*()890@192.168.62.53:1521/SHIRNOP'

# 本地文件路径
insert_file_path = r'D:\Hadoop\PyFloder\email_test\src\class_test\data'
# D:\\IdeaProjects\\Python_service_daily\\src\\class_test

# 待入库文件名(本地测试)
insert_file_name = 'test.csv'

# 若无文件名，则需拼接路径+文件名
# 入库路径+文件名
insert_csv_file = insert_file_path + '\\' + insert_file_name
