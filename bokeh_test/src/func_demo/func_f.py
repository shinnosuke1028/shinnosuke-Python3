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


def default_f(a, data=[1]):
    # 不要使用可变对象作为函数的默认入口参数
    # 可变对象在函数被实例化时，初始化仅发生在首次，所以可变参数会在反复调用函数的过程中，不断累积
    data.append(a)
    return data


# 如果非要使用可变对象作为入参，则使用 default_f_2 的形式，在反复实例化的过程中完成可变参数的初始化
def default_f_2(a, data=None):
    if data is None:
        data = []
    data.append(a)
    return data


# 一般对象不同于上述可变对象
def default_f_origin(a = None, data = None):
    return a, data


if __name__ == '__main__':
    # date_f(-1)
    # move(3,'A','B','C')

    print('第1次调用：', default_f(2))  # [1, 2]
    print('第2次调用：', default_f(3), '\n')  # [1, 2, 3]

    print('第1次调用：', default_f_origin())  # [None, None]
    print('第2次调用(不受首次影响)：', default_f_origin(1,2))  # [1, 2]
    print('第3次调用(不受首次影响)：', default_f_origin(3,4))  # [3, 4]
