# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/9/5 8:08
# @File: class_email_daily.py

import datetime
import os

import src.conf.bas_insert_conf


def date_f(timedelta = 0):
    # date_str = datetime.datetime.now().strftime('%Y%m%d')
    date_str = (datetime.datetime.now() + datetime.timedelta(days=timedelta)).strftime('%Y%m%d')
    date_str_s = (datetime.datetime.now() + datetime.timedelta(days=timedelta)).strftime('%Y%m%d %H:%M:%S')
    # print(datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'))\
    # print('PKG：' + __name__)
    # print(date_str + '\n')
    return date_str, date_str_s, __name__


def move(n, a, b, c):
    if n == 1:
        print(a + '-->' + c)
    else:
        move(n-1,a, c, b)
        move(1, a, b, c)
        move(n-1, b, a, c)


def file_rm_f(local_file_path):
    for path in os.walk(local_file_path):
        print(path)
        for file_cur in path[-1]:
            print(file_cur)   # ['20190905_Gather.csv', '20190905_PKG.csv', '20190906_PKG.csv']
            if file_cur.split('_')[0] != date_f(0)[0]:
                os.remove(path[0] + '\\' + file_cur)
            else:
                pass


# if __name__ == '__main__':
#     # date_f(-1)
#     # move(3,'A','B','C')