# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:34
# @File: ftp_conf.py
# @Usage: Configuration

"""

@FTP Connection Configuration BAK

"""
import os.path
import pprint
import pandas as pd


# Self Repo
from src.func_demo.func_f import date_f


# 时间戳准备
y = date_f()[3]['year']
m = date_f()[3]['month']
d = date_f()[3]['day']
h = date_f()[3]['hour']


# FTP登录信息配置
ftp_ip_dict = {
    # LOCAL
    # 'LOCAL': {
    #     {
    #         'host': '192.168.73.1',
    #         'port': 21,
    #         'usr': '1320964752@qq.com',
    #         'passwd': 'ghr921028',
    #         'remotePath': f'/test/{y}{m}{d}',
    #         're_pattern': f'.*{y}{m}{d}.*.gz$'      # Ex: *.20191210.csv
    #     }, {
    #         'host': '192.168.73.1',
    #         'port': 21,
    #         'usr': '1320964752@qq.com',
    #         'passwd': 'ghr921028',
    #         'remotePath': f'/test/{y}{m}{d}',
    #         're_pattern': f'.*{y}{m}{d}.*.gz$' }    # Ex: *.20191210.csv
    # },

    'HW_CM': (
        {
            'host': '172.2.3.30',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Admin@123',
            'remotePath': '/opt/oss/server/var/fileint/cm/autoExport/',
            're_pattern': f'CMExport_F.*._{y}{m}{d}.*?xml'
        }, {
            'host': '172.2.3.40',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': '/opt/oss/server/var/fileint/cm/autoExport/',
            're_pattern': f'CMExport_F.*._{y}{m}{d}.*?xml'
        }, {
            'host': '172.16.199.11',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': f'/opt/oss/server/var/itf_n/ftpFile/SH/LTE/MOBILE/HUAWEI/OMC1/CM/{y}{m}{d}/',
            're_pattern': f'HW_CM_{y}{m}{d}.*?tar.gz$'
        }, {
            'host': '172.16.198.8',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': f'/opt/oss/server/var/fileint/cm/autoExport/',
            're_pattern': f'CMExport_F.*.{y}{m}{d}.*?xml.gz$'
        }, {
            'host': '172.16.201.11',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': f'/opt/oss/server/var/itf_n/ftpFile/SH/LTE/MOBILE/HUAWEI/OMC2/CM/{y}{m}{d}',
            're_pattern': f'HW_CM_{y}{m}{d}.*?tar.gz$'
        }, {
            'host': '172.16.201.12',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': f'/opt/oss/server/var/itf_n/ftpFile/SH/LTE/MOBILE/HUAWEI/OMC2/CM/{y}{m}{d}',
            're_pattern': f'HW_CM_{y}{m}{d}.*?tar.gz$'
        }, {
            'host': '172.16.201.13',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': f'/opt/oss/server/var/itf_n/ftpFile/SH/LTE/MOBILE/HUAWEI/OMC2/CM/{y}{m}{d}',
            're_pattern': f'HW_CM_{y}{m}{d}.*?tar.gz$'}
    ),

    # HW CM 锚点站
    'HW_CM_NSA': (
        {
            'host': '172.16.201.11',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': f'/opt/oss/server/var/itf_n/ftpFile/SH/4GNSA/MOBILE/HUAWEI/OMC2/CM/{y}{m}{d}',
            're_pattern': f'HW_CM_{y}{m}{d}.*?tar.gz$'
        }, {
            'host': '172.16.201.12',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': f'/opt/oss/server/var/itf_n/ftpFile/SH/4GNSA/MOBILE/HUAWEI/OMC2/CM/{y}{m}{d}',
            're_pattern': f'HW_CM_{y}{m}{d}.*?tar.gz$'
        }, {
            'host': '172.16.201.13',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': f'/opt/oss/server/var/itf_n/ftpFile/SH/4GNSA/MOBILE/HUAWEI/OMC2/CM/{y}{m}{d}',
            're_pattern': f'HW_CM_{y}{m}{d}.*?tar.gz$'
        }, {
            'host': '172.16.199.11',
            'port': 21,
            'usr': 'ftpuser',
            'passwd': 'Changeme_123',
            'remotePath': f'/opt/oss/server/var/itf_n/ftpFile/SH/4GNSA/MOBILE/HUAWEI/OMC1/CM/{y}{m}{d}',
            're_pattern': f'HW_CM_{y}{m}{d}.*?tar.gz$'},
    ),

    # NSN CM
    'NSN_CM': (
        {
            'host': '111.222.6.44',
            'port': 21,
            'usr': 'ftirpuser',
            'passwd': 'Ftirpuser123!',
            'remotePath': '/cuc/cm/',
            're_pattern': f'.*{y}{m}{d}.*?gz$'
        }, {
            'host': '111.222.123.44',
            'port': 21,
            'usr': 'mqm',
            'passwd': 'Acbd9876_',
            'remotePath': '/d/oss/global/var/ftirpuser/cuc/cm/',
            're_pattern': f'.*{y}{m}{d}.*?gz$'
        }, {
            'host': '111.222.124.44',
            'port': 21,
            'usr': 'ftirpuser',
            'passwd': 'Acbd9876_',
            'remotePath': '/cuc/cm/',
            're_pattern': f'.*{y}{m}{d}.*?gz$'
        }, {
            'host': '111.222.6.146',
            'port': 21,
            'usr': 'ftirpuser',
            'passwd': 'Ftirpuser123!',
            'remotePath': '/cuc/cm/',
            're_pattern': f'.*{y}{m}{d}.*?gz$'
        }, {
            'host': '172.16.210.18',
            'port': 21,
            'usr': 'ftirpuser',
            'passwd': 'Ftirpuser123!',
            'remotePath': '/cuc/cm/',
            're_pattern': f'.*{y}{m}{d}.*?gz$' }
    )
}


# 清单相关配置
# 清单标题
titleDict = {
    'DEFAULT': ('flag', '86_NSN_CM')
}

# 生成路径
file_nlst_path = './data_output/'

# 清单标签
fileDict = {
    'LOCAL': {
        'title': ('flag', 'LOCAL'),
        'flag': 'FileList'
    },
    'HW_CM': {
        'title': ('flag', '85_HW_CM'),
        'flag': '85_HW_CM'
    },
    'HW_CM_NSA': {
        'title': ('flag', '85_HW_CM_NSA'),
        'flag': '85_HW_CM'
    },
    'NSN_CM': {
        'title': ('flag', '86_NSN_CM'),     # '86_NSN_CM'
        'flag': '86_NSN_CM'
    },
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
if __name__ == '__main__':
    # print(f'/test/{y}{m}{d}{h}')
    # print(date_f())

    # 查看默认打印宽度
    print(f'Output width: {pd.options.display.width}')
    print(f'Max_colwidth: {pd.options.display.max_colwidth}')

    # 设置打印的最大列数/行数/单列最大宽度(单列不省略)/总列宽(多列不换行)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', 100)
    pd.set_option('display.width', 200)
    # 方法2
    # pd.options.display.max_columns = None
    # pd.options.display.max_rows = None
    # pd.options.display.max_colwidth = 100
    # pd.options.display.width = 150

    for rn_dict in ftp_ip_dict.keys():
        # pprint.pprint(ftp_ip_dict[rn_dict])
        print(f'{rn_dict}: {ftp_ip_dict}')

        # 伪扁平化，字典转Dataframe
        # for rs in ftp_ip_dict[rn_dict]:
        #     pprint.pprint(rs)  # <class 'dict'>
        #     frame = pd.DataFrame(rs, index=['host'])
        #     print(frame)

        frame = pd.DataFrame(ftp_ip_dict[rn_dict])
        x = pd.DataFrame()
        x['path'] = frame['remotePath']
        frame = frame.drop(['remotePath'], axis=1)
        frame['path'] = x['path']
        del x
        print(frame)
        # print(x)
