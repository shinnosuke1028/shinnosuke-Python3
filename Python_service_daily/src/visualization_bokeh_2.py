# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/11/26 10:44
# @File: visualization_bokeh.py

from bokeh.plotting import output_file, figure, show
from bokeh.models import ColumnDataSource
import pandas as pd
from pandas import DataFrame
import numpy as np
import sys
import os
import copy
import shutil

from src.func_test.func_f import date_f
# from ..func_test.func_f import date_f
from src.conf import bas_mail_conf
# from conf import bas_insert_conf
from src.conf import sql_conf
import shutil


def df_data_frame(data_df, column, loc_flag):
    df_list = []
    for r1 in loc_flag:
        df_temp = data_df[data_df[column] == r1]   # loc_flag = (data['STYLE'] == 'NSN_4G_PM')
        df_list.append(df_temp)
    return df_list


def source_dict(*args):
    data_dict = dict(
        x=args[0],
        y=args[1],
        flag=args[2]
    )
    return data_dict


def file_copy(path_in, path_target):
    # adding exception handling
    try:
        shutil.copy(path_in, path_target)
    except IOError as e:
        print('Unable to copy file. %s' % e)
        exit(1)
    except:
        print("Unexpected errors:", sys.exc_info())
        exit(1)


if __name__ == '__main__':
    # sys.path.append(".")
    # print(sys.path)
    # html_path2 = os.path.abspath('.')   # 表示当前所处的文件夹的绝对路径：D:\Hadoop\PyFloder\email_test\src\class_test

    # print(os.getcwd())   # 表示当前所处的文件夹的绝对路径

    # 数据拷贝
    # data_source = input("Enter source file with full path: ")
    # target = input("Enter target file with full path: ")
    file_copy(path_in=bas_mail_conf.data_origin, path_target=bas_mail_conf.data_path)

    # HTML生成路径
    html_path = bas_mail_conf.html_output_path + date_f()[0] + '_log_lines.html'
    # print(html_path)
    # visual_html(html_path)

    # 数据源路径
    # source_data_path = bas_mail_conf.data_path + date_f(-4)[0] + '_GATHER.csv'
    source_data_path = bas_mail_conf.data_source
    # print(source_data_path)

    # data = pd.read_csv(data_path, encoding='ANSI')
    source_data_df = pd.read_csv(source_data_path,
                                 usecols=['DATA_STYLE', 'HOUR', 'NORMAL_FILE_SIZE/MB', 'NOW_SIZE/MB'],
                                 # index_col='HOUR',
                                 # header=None,
                                 # sep=',',
                                 index_col=['HOUR'])  # 这里设置列索引后，后续不需要重复设置 # 1

    # print(data)
    # print(type(data), id(data))
    dic_col = {'DATA_STYLE': 'STYLE', 'NORMAL_FILE_SIZE/MB': 'NORMAL', 'NOW_SIZE/MB': 'NOW'}
    df_rename = source_data_df.rename(columns=dic_col)  # 2
    # print(df_rename)
    # print(id(df_rename))
    del source_data_df  # <------

    # print(type(data_rename['STYLE'] == 'NSN_4G_PM'))
    # <class 'pandas.core.series.Series'> 类似一维数组,返回布尔结果：False/True

    # 定义筛选列
    column_flag = 'STYLE'
    # 定义筛选值
    owner_flag = ('NSN_4G_PM', 'HW_4G_PM<OMC1>', 'HW_4G_PM<OMC2>', 'NSN_4G_CM', 'HW_4G_CM<OMC1>', 'HW_4G_CM<OMC2>')
    owner_color = ('RED', 'ORANGE', 'SKYBLUE', 'GREEN', 'CHOCOLATE', 'BLUEVIOLET')
    # 数据筛选
    df_filter_1 = df_data_frame(data_df=df_rename, column=column_flag, loc_flag=owner_flag)     # <class 'list'>

    # Bokeh HTML输出路径
    output_file(html_path)

    # list 封装各线图源数据
    # pm_nsn = pm_hw1 = pm_hw2 = cm_nsn = cm_hw1 = cm_hw2 = None
    df_owner = []
    for rs in df_filter_1:
        # print('第%s组 DataFrame:' % n, '\n', rs)
        df_tmp = ColumnDataSource(data=source_dict(rs.index, rs['NOW'], rs['STYLE']))
        # print(rs['STYLE'].loc[[0]]) # 等价于 print(rs['STYLE'].[0:1]) 含索引列，返回的是一个
        # print(type((rs['STYLE'].loc[[0]]))) # <class 'pandas.core.series.Series'>
        # print(type((rs['STYLE'].loc[0]))) # <class 'str'>

        # print(rs['STYLE'].loc[0])
        str_flag = rs['STYLE'].loc[0]   # 仅取一行作为这一类的标签，不然后续判断时会报错

        # print(rs['STYLE'])
        # print(df_tmp)
        if str_flag == owner_flag[0]:
            # pm_nsn = df_tmp     # <class 'bokeh.models.sources.ColumnDataSource'> == dict
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[1]:
            # pm_hw1 = df_tmp
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[2]:
            # pm_hw2 = df_tmp
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[3]:
            # cm_nsn = df_tmp
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[4]:
            # cm_hw1 = df_tmp
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[5]:
            # cm_hw2 = df_tmp
            df_owner.append(df_tmp)

    # pm_nsn = ColumnDataSource(data=source_dict(df_filter_1[0].index, df_filter_1[0]['NOW'], df_filter_1[0]['STYLE']))
    # pm_hw1 = ColumnDataSource(data=source_dict(df_filter_1[1].index, df_filter_1[1]['NOW'], df_filter_1[1]['STYLE']))
    # pm_hw2 = ColumnDataSource(data=source_dict(df_filter_1[2].index, df_filter_1[2]['NOW'], df_filter_1[2]['STYLE']))

    tool = [
        ("FILE_SIZE", "@{y}{0.2f}MB"),
        # ("HOUR", "$index"),
        ("HOUR", "@x"),
        ("FILE_STYLE", "@flag")
    ]

    fig = figure(
        tooltips=tool,  # [("x", "$x"), ("y", "$y")],
        # plot_width=1600,
        # plot_height=674,
        plot_width=1300,
        plot_height=674,
        title='Example Glyphs',
        x_axis_label='HOUR',
        y_axis_label='FILE_SIZE/MB',
    )
    fig.x_range.range_padding = fig.y_range.range_padding = 0.1
    # p.y_range.end = 2200
    # p.y_range.max_interval = 100

    # 样例输出
    # print(pm_nsn.data['x'], '\n', pm_nsn.data['y'], '\n', pm_nsn.data['flag'].loc[0])

    # 以下为未封装的图形绘制
    # fig.line(x='x', y='y', legend=owner_flag[0], line_width=3, source=pm_nsn)
    # fig.circle(x='x', y='y', legend=owner_flag[0], size=10, source=pm_nsn)
    #
    # fig.line(x='x', y='y', source=pm_hw1, legend=owner_flag[1], line_width=3, color='ORANGE')
    # fig.circle(x='x', y='y', source=pm_hw1, legend=owner_flag[1], line_color='ORANGE', fill_color='ORANGE', size=10)
    #
    # fig.line(x='x', y='y', source=pm_hw2, legend=owner_flag[2], line_width=3, color='RED')
    # fig.circle(x='x', y='y', source=pm_hw2, legend=owner_flag[2], line_color='RED', fill_color='RED', size=10)

    # 以下为半封装的图形绘制
    i = 0
    for rs in df_owner:
        if rs.data['flag'].loc[0] in owner_flag[0:3]:
            # fig.line(x='x', y='y', legend=owner_flag[0], line_width=3, source=pm_nsn)
            # fig.line(x='x', y='y', legend=owner_flag[0], line_width=3, source=pm_nsn)
            fig.line(x='x', y='y', legend=rs.data['flag'].loc[0], line_width=3, source=rs, color=owner_color[i])
            fig.circle(x='x', y='y', legend=rs.data['flag'].loc[0], size=10, source=rs, line_color=owner_color[i],
                       fill_color=owner_color[i])
            i += 1
        elif rs.data['flag'].loc[0] in owner_flag[3:6]:
            fig.square_x(x='x', y='y', legend=rs.data['flag'].loc[0], line_width=2, size=15, source=rs,
                         color=owner_color[i], fill_color=None)
            # print(rs.data)
            i += 1

    show(fig)

    # print(bas_mail_conf.mail_file_path)
    # html_line(source_data_path, )
