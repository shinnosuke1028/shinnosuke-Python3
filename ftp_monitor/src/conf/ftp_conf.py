# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:34
# @File: ftp_conf.py
# @Usage: Configuration

"""
@FTP Connection Configuration
"""
from func_demo.func_f import date_f


# 时间戳准备
year = date_f()[3]['year']
month = date_f()[3]['month']
day = date_f()[3]['day']
hour = date_f()[3]['hour']


# FTP登录信息配置
ftp_conf_dict = (
    {
        'host': '192.168.73.1',
        'port': 21,
        'usr': '1320964752@qq.com',
        'passwd': 'ghr921028',
        'remotePath': f'/test/{year}{month}{day}{hour}',
    },{
        'host': '192.168.73.1',
        'port': 21,
        'usr': '1320964752@qq.com',
        'passwd': 'ghr9210',
        'remotePath': '/test/',
    }
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
    print(f'/test/{year}{month}{day}{hour}')
    # print(date_f())
    # print(year)
    print(ftp_conf_dict[0]['remotePath'])
