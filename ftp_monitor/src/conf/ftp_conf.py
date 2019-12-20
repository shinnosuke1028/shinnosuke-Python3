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
        'host': '172.16.210.18',
        # 'port': 21,
        'usr': 'ftirpuser',
        'passwd': 'Ftirpuser123!',
        'remotePath': '/cuc/cm/',
    },
    {
        'host': '111.222.6.146',
        # 'port': 21,
        'usr': 'ftirpuser',
        'passwd': 'Ftirpuser123!',
        'remotePath': '/cuc/cm/',
    }, {
        'host': '111.222.123.44',
        # 'port': 21,
        'usr': 'mqm',
        'passwd': 'Acbd9876_',
        'remotePath': '/d/oss/global/var/ftirpuser/cuc/cm/',
    }, {
        'host': '111.222.124.44',
        # 'port': 21,
        'usr': 'ftirpuser',
        'passwd': 'Acbd9876_',
        'remotePath': '/cuc/cm/',
    }, {
        'host': '111.222.6.44',
        # 'port': 21,
        'usr': 'ftirpuser',
        'passwd': 'Ftirpuser123!',
        'remotePath': '/cuc/cm/',
    }
)


# {
#     'host': '192.168.73.1',
#     'port': 21,
#     'usr': '1320964752@qq.com',
#     'passwd': 'ghr921028',
#     'remotePath': f'/test/{y}{m}{d}{h}',
# }, {
#     'host': '192.168.73.1',
#     'port': 21,
#     'usr': '1320964752@qq.com',
#     'passwd': 'ghr9210',
#     'remotePath': '/test/',
# },


# 清单生成
file_nlst_path = './data_output/'

fileDict = {
    'LOCAL': {
    'title': 'LOCAL',
    'flag': 'FileList'
    },
    'HW_CM': {
    'title': 'HW_CM',
    'flag': '85_HW_CM'
    }
}


# Demo
# if __name__ == '__main__':
#     # print(date_f())
#     # print(year)
#     print(ftp_ip_dict[1]['remotePath'])
