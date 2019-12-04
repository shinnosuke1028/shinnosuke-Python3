# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/10/9 9:45
# @File: hr_temp_2.py

import time
from tqdm import tqdm
from time import sleep

# a = None    # 全局变量
#
#
# def fun():
#     global a    # 使用之前在全局里定义的 a
#     c = None
#     a = 20      # 现在的 a 是全局变量了
#     c = 30
#     return a+100
#
#
# print('a past:', a)     # None
# b = fun()
# print('a now:', a)      # 20
# a = 35
# print('a last:', a)     # 全局变量在函数内外都可修改，安全度降低
# print(b)
# print(c)    # c为局部变量，无法调用
#


# 类私有变量访问
class Money(object):
    def __init__(self):
        self.__money = 0

    def get_money(self):
        return self.__money

    def set_money(self, value):
        if isinstance(value, int):
            self.__money = value
        else:
            print("error:不是整型数字")

    # 定义一个属性，当对这个money设置值时调用setMoney,当获取值时调用getMoney
    money = property(get_money, set_money)

# a = Money()
# a.money = 100  # 调用setMoney方法
# print(a.money)  # 调用getMoney方法
######################################################

######################################################
# 多层装饰器


# print(1)
#
#
# def func(f):
#     print(2)
#
#     def inner_func(*name1, **name2):
#         print('{}函数开始运行……'.format(f.__name__))
#         f(*name1, **name2)
#         print('{}函数结束运行……'.format(f.__name__))
#     print(3)
#     return inner_func
#
#
# print(4)
#
#
# def timer(f):
#     print(5)
#
#     def inner_timer(*args, **kwargs):
#         print('开始计时……')
#         start_time = time.time()
#         f(*args, **kwargs)
#         end_time = time.time()
#         print('开始结束……')
#         time_cost = end_time - start_time
#         print('{}函数运行时长为：{}秒'.format(f.__name__, time_cost))
#     print(6)
#     return inner_timer
#
#
# print(7)
#
#
# @func
# @timer
# def do_something(name):
#     time.sleep(1)
#     print('你好，{}！'.format(name))
#
#
# print(8)


def do_something_2(name):
    time.sleep(1)
    print('你好，{}！'.format(name))
######################################################


if __name__ == '__main__':
    # print(9)
    # do_something(name='姚明')
    # print('-------------------------')
    # func(timer(do_something_2))(name='姚明')  # 执行效果与上面使用了@符号的do_something一样
    #####################################################

    # pbar = tqdm(["a", "b", "c", "d"])
    # for char in pbar:
    #    # pbar.set_description("Processing %s" % char)
    #    sleep(1)
    #####################################################

    # league = [('a', 1), ('a', 2), ('b', 3), ('b', 4), ('c', 5)]
    # d = {}
    # for k, v in league:
    #     if k in d:
    #         d[k].append(v)
    #     else:
    #         d[k] = [v]
    # for i in d.items():
    #     print(i)
    #####################################################

    # 多字典合并
    # list_of_ds = [
    #     {'a': [1, 2], 'b': [4, 5], 'c': [6, 7]},
    #     {'a': [4], 'b': [56], 'c': [46]},
    #     {'a': [92], 'b': [65], 'c': [43]}
    # ]
    #
    # new = {}
    # for d in list_of_ds:
    #     for i, j in d.items():
    #         new.setdefault(i, []).extend(j)
    # print(new)
    #####################################################

    #####################################################
    # 多线图，但第2条线图为两条子线图的和(Y轴求和)
    from bokeh.models import ColumnDataSource
    from bokeh.plotting import figure, output_file, show

    tools = [
        ("FILE_SIZE", "@y"),
        ("HOUR", "@x"),
    ]
    output_file("vline_stack.html")

    source = ColumnDataSource(data=dict(
        x=[1, 2, 3, 4, 5],
        y1=[1, 2, 4, 3, 4],
        y2=[1, 4, 2, 2, 3],
    ))
    p = figure(plot_width=1300, plot_height=674)

    # print(source.data)
    # print(source.data['x'])
    # print(source.data['y1'])
    # print(source.data['y2'])

    p.multi_line(xs=[source.data['x'], source.data['x']], ys=[source.data['y1'], source.data['y2']],
                 color=['ROYALBLUE', 'RED'], line_width=[3, 3])
    p.circle(x=source.data['x'], y=source.data['y1'], size=10)
    p.circle(x=source.data['x'], y=source.data['y2'], size=10, fill_color='RED', line_color='RED')
    show(p)
    #####################################################
