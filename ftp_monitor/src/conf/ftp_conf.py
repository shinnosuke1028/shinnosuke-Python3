# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:34
# @File: ftp_conf.py
# @Usage: Configuration

"""

@FTP Connection Configuration

"""
# Self Repo
from src.func_demo.func_f import date_f


# 时间戳准备
y = date_f()[3]['year']
m = date_f()[3]['month']
d = date_f(-2)[3]['day']
h = date_f()[3]['hour']


# FTP登录信息配置
ftp_ip_dict = {
    'LOCAL_1':(
        {
            'host': '192.168.73.1',
            'port': 21,
            'usr': '1320964752@qq.com',
            'passwd': 'ghr921028',
            'remotePath': f'/test/20190800',
        }, {
            'host': '192.168.73.1',
            'port': 21,
            'usr': '1320964752@qq.com',
            'passwd': 'ghr921028',
            'remotePath': f'/test/201908',
        }
    ),
    'LOCAL_2':(
        {
            'host': '192.168.73.1',
            'port': 21,
            'usr': '1320964752@qq.com',
            'passwd': 'ghr921028',
            'remotePath': f'/test/{y}{m}{d}{h}',
        }, {
            'host': '192.168.73.1',
            'port': 21,
            'usr': '1320964752@qq.com',
            'passwd': 'ghr921028',
            'remotePath': f'/test/{y}{m}{d}',
        }
    )
}


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
    # print(f'/test/{year}{month}{day}{hour}')
    # print(date_f())
    # print(ftp_ip_dict)
    key_map = []
    for key in ftp_ip_dict.keys():
        # print(f'key: {key}')
        key_map.append(key)
    # print(keys_map)

    for key in key_map:
        # print(ftp_ip_dict[key], '\n')
        for rs in ftp_ip_dict[key]:
            print(f'{key}: {rs}')
        print('\n')
