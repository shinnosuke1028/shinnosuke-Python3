# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:34
# @File: ftp_conf.py
# @Usage: Configuration

"""

@FTP Connection Configuration BAK

"""
import os.path


# Self Repo
from src.func_demo.func_f import date_f


# 时间戳准备
y = date_f()[3]['year']
m = date_f()[3]['month']
d = date_f()[3]['day']
h = date_f()[3]['hour']


# FTP登录信息配置
ftp_ip_dict = (
    {
        'host': '192.168.73.1',
        'port': 21,
        'usr': '1320964752@qq.com',
        'passwd': 'ghr921028',
        'remotePath': f'/test/{y}{m}{d}',
        're_rule': f'.*{y}{m}{d}.*.csv$'   # Ex: *.20191210.csv
    }, # {
    #     'host': '192.168.73.1',
    #     'port': 21,
    #     'usr': '1320964752@qq.com',
    #     'passwd': 'ghr921028',
    #     'remotePath': f'/test/{y}{m}{d}',
    # },

    # HW CM
    # {
    #     'host': '172.2.3.30',
    #     'port': 21,
    #     'usr': 'ftpuser',
    #     'passwd': 'Admin@123',
    #     'remotePath': '/opt/oss/server/var/fileint/cm/autoExport/',
    # }, {
    #     'host': '172.2.3.40',
    #     'port': 21,
    #     'usr': 'ftpuser',
    #     'passwd': 'Changeme_123',
    #     'remotePath': '/opt/oss/server/var/fileint/cm/autoExport/',
    # }, {
    #     'host': '172.16.201.11',
    #     'port': 21,
    #     'usr': 'ftpuser',
    #     'passwd': 'Changeme_123',
    #     'remotePath': '/opt/oss/server/var/fileint/cm/autoExport/',
    # }, {
    #     'host': '172.16.201.12',
    #     'port': 21,
    #     'usr': 'ftpuser',
    #     'passwd': 'Changeme_123',
    #     'remotePath': '/opt/oss/server/var/fileint/cm/autoExport/',
    # }, {
    #     'host': '172.16.201.13',
    #     'port': 21,
    #     'usr': 'ftpuser',
    #     'passwd': 'Changeme_123',
    #     'remotePath': '/opt/oss/server/var/fileint/cm/autoExport/',
    # }, {
    #     'host': '172.16.199.11',
    #     'port': 21,
    #     'usr': 'ftpuser',
    #     'passwd': 'Changeme_123',
    #     'remotePath': '/opt/oss/server/var/fileint/cm/autoExport/',
    # }, {
    #     'host': '172.16.198.8',
    #     'port': 21,
    #     'usr': 'ftpuser',
    #     'passwd': 'Changeme_123',
    #     'remotePath': '/opt/oss/server/var/fileint/cm/autoExport/',
    # }, {
    #     'host': '',
    #     'port': 21,
    #     'usr': '',
    #     'passwd': '',
    #     'remotePath': '',
    # },
)

# 清单相关配置
# 生成路径
file_nlst_path = './data_output/'

#清单标签
fileDict = {
    'LOCAL':{
        'title': 'LOCAL',
        'flag': 'FileList'
    },
    'HW_CM':{
        'title': 'LOCAL',
        'flag': '85_HW_CM'
    }
}

# 测试用文件&文件夹生成
# 文件名
file_name_1 = f'34G_output_{date_f()[1]}_tmp.csv'
file_name_2 = f'5G_output_{date_f()[0]}.xml.gz'
file_name_3 = f'5G_output_{date_f()[1]}.csv'

# 文件名列表
file_name_list = [file_name_1, file_name_2, file_name_3]
# 文件路径
# f_path = rf'../../../../../FTP/test/{date_f()[0]}/'    # D:\Hadoop\PyFloder\ftp_monitor\src\func_demo\
f_path = os.path.abspath(f'../../../../../FTP/test/{date_f()[0]}') + '\\'  # 表示当前所处项目的绝对路径：D:\Hadoop\PyFloder\bokeh_test

# 文件夹名
d_name = f'{date_f()[0]}'
# 文件夹路径
d_path = os.path.abspath(f'../../../../../FTP/test') + '\\'


# # Demo
# if __name__ == '__main__':
#     print(f'/test/{y}{m}{d}{h}')
#     # print(date_f())
#     # print(year)
#     print(ftp_ip_dict[0]['remotePath'])
