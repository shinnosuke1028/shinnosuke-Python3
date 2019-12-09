# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:34
# @File: ftp_conf.py
# @Usage: Configuration

"""
@FTP Connection Configuration
"""
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
        'remotePath': f'/test/{y}{m}{d}{h}',
    },
    # {
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

# 清单生成
file_nlst_path = './data_output/'

fileDict = {
    'LOCAL':{
    'title': 'LOCAL',
    'flag': 'FileList'
    },
    'HW_CM':{
    'title': 'HW_CM',
    'flag': '85_HW_CM'
    }
}


# Demo
if __name__ == '__main__':
    print(f'/test/{y}{m}{d}{h}')
    # print(date_f())
    # print(year)
    print(ftp_ip_dict[0]['remotePath'])
