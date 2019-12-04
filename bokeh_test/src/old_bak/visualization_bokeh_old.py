# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/12/2 10:43
# @File: visualization_bokeh_old.py

from bokeh.plotting import output_file, figure, show
from bokeh.models import ColumnDataSource
import pandas as pd
from pandas import DataFrame
import numpy as np
import sys, os
import copy

from src.func_demo.func_f import date_f
# from ..func_test.func_f import date_f
from src.conf import bas_mail_conf
# from conf import bas_insert_conf
from src.conf import sql_conf

# data_path = r'./data/'
# data_path = data_path + date_f(-1)[0] + '_GATHER.csv'
# # print(data_path)
# # print(sys.path)
#
# # data = pd.read_csv(data_path, encoding='ANSI')
# data = pd.read_csv(data_path,
#                    usecols=['DATA_STYLE', 'HOUR', 'NORMAL_FILE_SIZE/MB', 'NOW_SIZE/MB'],
#                    # index_col='HOUR',
#                    # header=None,
#                    # sep=',',
#                    index_col=['HOUR']   # 这里设置列索引后，后续不需要重复设置 # 1
#                    )
#
# # print(data)
# # print(type(data), id(data))
# dic_col = {'DATA_STYLE': 'STYLE', 'NORMAL_FILE_SIZE/MB': 'NORMAL', 'NOW_SIZE/MB': 'NOW'}
# data_rename = data.rename(columns=dic_col)  # 2
# # print(data_rename)
# # print(id(data_rename))
# del data  # <------
#
# # 按照列值筛选
# # data_DataFrame = data_rename.loc[data_rename['STYLE'] == 'NSN_4G_PM'].set_index('HOUR')
# df_nsn = data_rename[data_rename['STYLE'] == 'NSN_4G_PM']   # .set_index('HOUR') # 1
# df_hw_pm1 = data_rename[data_rename['STYLE'] == 'HW_4G_PM<OMC1>']
# df_hw_pm2 = data_rename[data_rename['STYLE'] == 'HW_4G_PM<OMC2>']
# df_nsn_cm = data_rename[data_rename['STYLE'] == 'NSN_4G_CM']
# df_hw_cm1 = data_rename[data_rename['STYLE'] == 'HW_4G_CM<OMC1>']
# df_hw_cm2 = data_rename[data_rename['STYLE'] == 'HW_4G_CM<OMC2>']
# # print(id(data_DataFrame))
# # print(data_rename)
# # print(type(df_hw_pm1))    # <class 'pandas.core.frame.DataFrame'>
# del data_rename
#
# # 时间轴截取
# col_hour = df_nsn.index
#
# # cols = {u'DATA_STYLE': '类型', u'NORMAL_FILE_SIZE/MB': '正常', u'NOW_SIZE/MB': '当前'}
# # data_DataFrame_2 = data_DataFrame.copy()
# # 不进行Copy时，会报链式赋值Warning，即更改的对象指向的其实是原始内存地址保存的变量，这里指向的是data
# # 但第二次实验时，又不报错了，原因是对先前的data进行了内存释放  ------>
# # 要么释放掉最原始的数据来源，即箭头指向处；要么在生成 DataFrame 时就进行浅拷贝,规避链式赋值   # 2
# # print(id(data_DataFrame_2))
# # data_DataFrame.rename(columns=cols, inplace=True) # 第二个参数使用时有疑问？？？


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


# Visual
def visual_html(local_file_path):
    output_file(local_file_path)

    # 在这里封装各线图源数据
    source_pm_nsn = ColumnDataSource(data=source_dict(df_nsn.index, df_nsn['NOW'], df_nsn['STYLE']))
    # print(type(source_pm_nsn))
    source_pm_hw1 = ColumnDataSource(data=source_dict(df_hw_pm1.index, df_hw_pm1['NOW'], df_hw_pm1['STYLE']))
    source_pm_hw2 = ColumnDataSource(data=source_dict(df_hw_pm2.index, df_hw_pm2['NOW'], df_hw_pm2['STYLE']))

    # source_pm_hw1 = ColumnDataSource(
    #     data=dict(
    #     x=col_hour,
    #     y=df_hw_pm1['NOW'],
    #     flag=df_hw_pm1['STYLE']
    #     )
    # )


    tools = [
        ("FILE_SIZE", "@{y}{0.2f}MB"),
        ("HOUR", "$index"),
        ("FILE_STYLE", "@flag")
    ]

    p = figure(
                tooltips=tools,  # [("x", "$x"), ("y", "$y")],
                plot_width=1600,
                plot_height=674,
                title='Example Glyphs',
                x_axis_label='HOUR',
                y_axis_label='FILE_SIZE/MB',
               )
    p.x_range.range_padding = p.y_range.range_padding = 0.1
    # p.y_range.end = 2200
    # p.y_range.max_interval = 100

    p.line(x='x', y='y', legend='NSN_4G_PM', line_width=3, source=source_pm_nsn)
    p.circle(x='x', y='y', legend='NSN_4G_PM', size=10, source=source_pm_nsn)

    p.line(x='x', y='y', source=source_pm_hw1, legend='HW_4G_PM_OMC1', line_width=3, color='ORANGE')
    p.circle(x='x', y='y', source=source_pm_hw1, legend='HW_4G_PM_OMC1', line_color='ORANGE', fill_color='ORANGE', size=10)

    p.line(x='x', y='y', source=source_pm_hw2, legend='HW_4G_PM_OMC2', line_width=3, color='RED')
    p.circle(x='x', y='y', source=source_pm_hw2, legend='HW_4G_PM_OMC2', line_color='RED', fill_color='RED', size=10)

    # p.line(x=col_hour, y=df_nsn['NORMAL'], legend='NSN_4G_PM', line_width=3)
    # p.circle(x=col_hour, y=df_nsn['NORMAL'], legend='NSN_4G_PM', size=10)

    # p.line(x=col_hour, y=df_hw_pm1['NORMAL'], legend='HW_4G_PM_OMC1', line_width=3, color='ORANGE')
    # p.circle(x=col_hour, y=df_hw_pm1['NORMAL'], legend='HW_4G_PM_OMC1',
    # line_color='ORANGE', fill_color='ORANGE', size=10)

    # p.line(x=col_hour, y=df_hw_pm2['NORMAL'], legend='HW_4G_PM_OMC2', line_width=3, color='RED')
    # p.circle(x=col_hour, y=df_hw_pm2['NORMAL'], legend='HW_4G_PM_OMC2', line_color='RED', fill_color='RED', size=10)

    # p.line(x=-1, y=df_nsn_cm['NORMAL'], legend='NSN_4G_CM', line_width=3, color='MAROON')

    # p.line(x=[0, 5], y=df_hw_cm1['NORMAL'], legend='HW_4G_CM_OMC1', line_width=3, color='magenta')

    # p.line(x=-1, y=df_hw_cm2['NORMAL'], legend='HW_4G_CM_OMC2', line_width=3, color='LIGHTSALMON')

    show(p)


if __name__ == '__main__':
    # sys.path.append(".")
    # print(sys.path)

    # print(os.getcwd())   # 表示当前所处的文件夹的绝对路径
    # html_path2 = os.path.abspath('.')   # 表示当前所处的文件夹的绝对路径：D:\Hadoop\PyFloder\email_test\src\class_test

    # HTML生成路径
    html_path = bas_mail_conf.html_output_path + date_f()[0] + '_log_lines.html'
    print(html_path)
    # visual_html(html_path)

    # 数据源路径
    # source_data_path = bas_mail_conf.data_path + date_f(-4)[0] + '_GATHER.csv'
    source_data_path = bas_mail_conf.data_path + '20191126_GATHER.csv'
    # source_data_path = r'D:\IdeaProjects\bokeh_test\src\data\20191126_GATHER.csv'
    print(source_data_path)

    # data = pd.read_csv(data_path, encoding='ANSI')
    source_data_df = pd.read_csv(source_data_path,
                                 usecols=['DATA_STYLE', 'HOUR', 'NORMAL_FILE_SIZE/MB', 'NOW_SIZE/MB'],
                                 # index_col='HOUR',
                                 # header=None,
                                 # sep=',',
                                 index_col=['HOUR']  # 这里设置列索引后，后续不需要重复设置 # 1
                                 )

    # print(data)
    # print(type(data), id(data))
    dic_col = {'DATA_STYLE': 'STYLE', 'NORMAL_FILE_SIZE/MB': 'NORMAL', 'NOW_SIZE/MB': 'NOW'}
    df_rename = source_data_df.rename(columns=dic_col)  # 2
    # print(data_rename)
    # print(id(data_rename))
    del source_data_df  # <------

    # print(type(data_rename['STYLE'] == 'NSN_4G_PM'))
    # <class 'pandas.core.series.Series'> 类似一维数组,返回布尔结果：False/True

    # 定义筛选列
    column_flag = 'STYLE'
    # 定义筛选值
    owner_flag = ('NSN_4G_PM', 'HW_4G_PM<OMC1>', 'HW_4G_PM<OMC2>', 'NSN_4G_CM', 'HW_4G_CM<OMC1>', 'HW_4G_CM<OMC2>')
    owner_color = ('RED', 'ORANGE', 'SKYBLUE', 'GREEN', 'CHOCOLATE', 'BLUEVIOLET')
    # 数据筛选
    df_filter_1 = df_data_frame(data_df=df_rename, column=column_flag, loc_flag=owner_flag) # <class 'list'>
    # print(type(df_filter_1))

    # Bokeh HTML输出路径
    output_file(html_path)

    # 封装各线图源数据
    pm_nsn = pm_hw1 = pm_hw2 = cm_nsn = cm_hw1 = cm_hw2 = None
    df_owner = []
    for rs in df_filter_1:
        # print('第%s组 DataFrame:' % n, '\n', rs)
        df_tmp = ColumnDataSource(data=source_dict(rs.index, rs['NOW'], rs['STYLE']))
        # print(rs['STYLE'].loc[[0]]) # 等价于 print(rs['STYLE'].[0:1]) 含索引列，返回的是一个
        # print(type((rs['STYLE'].loc[[0]]))) # <class 'pandas.core.series.Series'>
        # print(type((rs['STYLE'].loc[0]))) # <class 'str'>

        # print(rs['STYLE'].loc[0])
        str_flag = rs['STYLE'].loc[0]
        print(str_flag)

        # print(df_tmp)
        if str_flag == owner_flag[0]:
            pm_nsn = df_tmp     # <class 'bokeh.models.sources.ColumnDataSource'> == dict
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[1]:
            pm_hw1 = df_tmp
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[2]:
            pm_hw2 = df_tmp
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[3]:
            cm_nsn = df_tmp
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[4]:
            cm_hw1 = df_tmp
            df_owner.append(df_tmp)
        elif str_flag == owner_flag[5]:
            cm_hw2 = df_tmp
            df_owner.append(df_tmp)

    # pm_nsn = ColumnDataSource(data=source_dict(df_filter_1[0].index, df_filter_1[0]['NOW'], df_filter_1[0]['STYLE']))
    # pm_hw1 = ColumnDataSource(data=source_dict(df_filter_1[1].index, df_filter_1[1]['NOW'], df_filter_1[1]['STYLE']))
    # pm_hw2 = ColumnDataSource(data=source_dict(df_filter_1[2].index, df_filter_1[2]['NOW'], df_filter_1[2]['STYLE']))

    tool = [
        ("FILE_SIZE", "@{y}{0.2f}MB"),
        ("HOUR", "$index"),
        ("FILE_STYLE", "@flag")
    ]

    fig = figure(
        tooltips=tool,  # [("x", "$x"), ("y", "$y")],
        plot_width=1600,
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

    i = 0
    for rs in df_owner:
        if rs.data['flag'].loc[0] in owner_flag[0:3]:
            # fig.line(x='x', y='y', legend=owner_flag[0], line_width=3, source=pm_nsn)
            # fig.line(x='x', y='y', legend=owner_flag[0], line_width=3, source=pm_nsn)
            fig.line(x='x', y='y', legend=rs.data['flag'].loc[0], line_width=3, source=rs, color=owner_color[i])
            fig.circle(x='x', y='y', legend=rs.data['flag'].loc[0], size=10, source=rs, line_color=owner_color[i], fill_color=owner_color[i])
            i += 1
        elif rs.data['flag'].loc[0] in owner_flag[3:6]:
            fig.square_x(x='x', y='y', legend=rs.data['flag'].loc[0], line_width=2, size=15, source=rs, color=owner_color[i], fill_color=None)
            # print(rs.data)
            i += 1

    # show(fig)

    # print(bas_mail_conf.mail_file_path)
    # html_line(source_data_path, )
    # pm_nsn = {}
