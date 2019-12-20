# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/8/31 12:40
# @File: hr_temp_1.py

from src.func_test.func_f import date_f
from time import sleep
from tqdm import tqdm
import copy

local_file_path = r'D:\FTP\Mail'


def list_f():
    list1 = [(1, 2, 3), (4, 5, 6)]
    # for i in range(len(list)):
    for i in list1:
        # rs = list[i]
        rs = i
        # print('对象%d的内容:' % (i+1), rs)
        print(i)
        # for j in range(len(rs)):
        for j in rs:
            # print('对象%d内的每个元素:' % (i+1), rs[j])
            print(j)


def row_insert_into_csc(title):
    # title = '执行时间，执行包，执行次数，最终数据结果'
    file_name = local_file_path + '\\' + date_f(0)[0] + '.csv'
    # print(file_name)
    file_1 = open(file_name, 'w')
    file_1.write(title)
    file_1.close()


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
    # list_f()

    # 进度条测试
    # progress(range(200))

    # 邮件名时间戳测试
    # title = '执行时间,执行包,执行次数,最终数据结果'
    # row_insert_into_csc(title)
    # date_f()

    # 深浅拷贝测试
    origin = [1, 2, [3, [4, 5]]]
    print('origin:', origin, '\n', 'id(origin):', id(origin))
    cop_temp = origin
    print('cop_temp:', cop_temp, '\n', 'id(cop_temp):', id(cop_temp))
    cop_temp1 = origin.copy()
    # 浅拷贝后，list内的对象开辟新内存保留，不受原始数据变化影响
    # 对象内的子对象不开辟新内存保留，还是指向原来对象内的子对象所在的内存，故跟着原始数据变化

    cop_temp2 = copy.deepcopy(origin)   # 深拷贝，对象内的对象及子对象均指向数据源原始内存，故不受原始数据变化影响
    origin[2][0] = 13
    origin[2][1][0] = 14
    origin[0] = 'a'
    print('origin修改后:', origin, '\n', 'id(origin):', id(origin))
    print('修改前进行浅拷贝:', cop_temp1, '\n', '修改前进行浅拷贝id:', id(cop_temp1))
    cop_temp1_1 = copy.copy(origin)
    print('修改后进行浅拷贝:', cop_temp1_1, '\n', '修改前进行浅拷贝id:', id(cop_temp1_1))
    # origin[2][0] ='hey!'
    # origin[0] = 'a'
    print('深拷贝:', cop_temp2, '\n', '深拷贝id:', id(cop_temp2))
    origin[2][1][1] = 15
    print('二次修改前深拷贝:', cop_temp2, '\n', '深拷贝id:', id(cop_temp2))
    print('origin二次修改后:', origin, '\n', 'id(origin):', id(origin))
    print('cop_temp:', cop_temp, '\n', 'id(cop_temp):', id(cop_temp))

    ######
    # print('第1次调用:', default_f(2))  # [1, 2]
    # print('第2次调用:', default_f(3), '\n')  # [1, 2, 3]
    #
    # print('第1次调用:', default_f_origin())  # [None, None]
    # print('第2次调用(不受首次影响):', default_f_origin(1,2))  # [1, 2]
    # print('第3次调用(不受首次影响):', default_f_origin(3,4))  # [3, 4]
