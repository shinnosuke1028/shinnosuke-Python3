# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/9/5 8:08
# @File: class_email_daily.py

import datetime
import os

# import src.conf.bas_insert_conf


def date_f(daysdelta=0, hoursdelta=0):
    """
    :param timedelta: Day Intervals
    :param hoursdelta: Hour Intervals
    :return: date <class:tuple>
            Ex: ('20191205', '2019120522', '20191205 22:40:51',
                 {'year': '2019', 'month': '12', 'day': '05', 'hour': '22'}, 'src.func_demo.func_f')
    """
    # date = []
    # date_str = datetime.datetime.now().strftime('%Y%m%d')

    date_str = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%Y%m%d')
    date_str_h = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%Y%m%d%H')
    date_str_s = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%Y%m%d %H:%M:%S')

    # 各维度时间戳段拆分
    year = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%Y')
    month = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%m')
    day = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%d')
    hour = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%H')
    date_dict = dict(year=year, month=month, day=day, hour=hour)

    # 所有已知时间格式组合
    date = (date_str, date_str_h, date_str_s, date_dict, __name__)
    return date


def file_rm_f(local_file_path):
    """
    :param local_file_path: 待检索文件夹
    :return:
    """
    for path in os.walk(local_file_path):
        print(path) # (<current path>, <dictionary in current path>, ['34G_output_2019121022_tmp.csv', '5G_output_20191210.xml.gz'])
        for file_cur in path[-1]:
            print(file_cur)   # ['34G_output_2019121022_tmp.csv', '5G_output_20191210.xml.gz']
            if file_cur.split('_')[0] != date_f(0)[0]:
                os.remove(path[0] + '\\' + file_cur)
                # pass
            else:
                pass


# if __name__ == '__main__':
# file_rm_f(r'D:\FTP\test\20191210')
#     # date_f(-1)
#     # move(3,'A','B','C')
