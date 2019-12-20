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

from src.func_demo.func_f import date_f
# from ..func_test.func_f import date_f
from src.conf import bas_mail_conf
# from conf import bas_insert_conf
from src.conf import bokeh_conf


def data_df_frame(data_df, column, value):
    df_list = []
    for r1 in value:
        df_temp = data_df[data_df[column] == r1]   # value = (data['STYLE'] == 'NSN_4G_PM')
        df_list.append(df_temp)
    return df_list


def source_dict(*args):
    data_dict = dict(
        x=args[0],
        y=args[1],
        flag=args[2]
    )
    return data_dict


def df_preparation(data_input_path, html_output_path, cols, cols_rename_dic,
                   info_filter,
                   index_col=None):
    """
    :param data_input_path: 数据路径
    :param html_output_path: HTML文件生成路径
    :param cols: 数据字段
        Ex: ['DATA_STYLE', 'HOUR', 'NORMAL_FILE_SIZE/MB', 'NOW_SIZE/MB']
    :param cols_rename_dic: 字段重命名
        Ex: {'DATA_STYLE': 'STYLE', 'NORMAL_FILE_SIZE/MB': 'NORMAL', 'NOW_SIZE/MB': 'NOW'}
    :param info_filter: 数据切片配置字典，按照示例排列字典 k,v
        Ex: {'column_flag': 'STYLE',    # 1 筛选字段
                     'owner_flag': ('NSN_4G_PM', 'HW_4G_PM<OMC1>', 'HW_4G_PM<OMC2>',
                                    'NSN_4G_CM', 'HW_4G_CM<OMC1>', 'HW_4G_CM<OMC2>'),   # 2 筛选值
                     'owner_color': ('RED', 'ORANGE', 'SKYBLUE', 'GREEN', 'CHOCOLATE', 'BLUEVIOLET'),   # 3
                     'y': 'NOW'}  # 4 根据自己需要展示的Y轴字段名来配置

    :param index_col: 数据索引字段
        Ex: ['HOUR']

    :return: DataFrame for HTML Bokeh   <class 'bokeh.models.sources.ColumnDataSource'>
    """
    # Initialization
    df_owner = []

    # Output Configuration
    output_file(html_output_path)

    # Data load
    try:
        data_df = pd.read_csv(data_input_path, usecols=cols, index_col=index_col)
        if cols_rename_dic is not None:
            data_df_rename = data_df.rename(columns=cols_rename_dic)
        else:
            data_df_rename = data_df
        del data_df
        print('Data load Successfully.')
        # print(data_df_rename)

    except IOError as e:
        print('Data load failure: %s' % e)

    else:
        # Data Filter
        k = []
        dict_range = len(info_filter)   # Ex: len(info_filter) = 4
        print(dict_range)
        for i in range(dict_range):
            k.append(sorted(info_filter.keys())[i])
            # Ex:
            # k1 = sorted(info_filter.keys())[0]
            # k2 = sorted(info_filter.keys())[1]
            # ...

        # # 定义筛选列
        # Ex:
        # column_flag = 'STYLE'
        # # 定义筛选值
        # owner_flag = ('NSN_4G_PM', 'HW_4G_PM<OMC1>', 'HW_4G_PM<OMC2>',
        #               'NSN_4G_CM', 'HW_4G_CM<OMC1>', 'HW_4G_CM<OMC2>')
        # owner_color = ('RED', 'ORANGE', 'SKYBLUE', 'GREEN', 'CHOCOLATE', 'BLUEVIOLET')
        # # 数据筛选
        # df_filter_1 = df_data_frame(data_df=df_rename, column='STYLE', loc_flag=owner_flag)     # <class 'list'>

        data_df_filter = data_df_frame(data_df=data_df_rename, column=info_filter[k[0]],
                                       value=info_filter[k[1]])     # <class 'list'>

        source_dict_flag = info_filter[k[0]]    # Ex: 'STYLE'
        source_dict_y = info_filter[k[dict_range - 1]]  # Ex: 'NOW'

        # 列表封装各线图源数据
        for rs in data_df_filter:
            df_tmp = ColumnDataSource(data=source_dict(rs.index, rs[source_dict_y], rs[source_dict_flag]))
            # <class 'bokeh.models.sources.ColumnDataSource'> == dict
            # Ex: df_tmp = ColumnDataSource(data=source_dict(rs.index, rs['NOW'], rs['STYLE']))

            # str_flag = rs[source_dict_flag].loc[0]   # 仅取一行作为这一类的标签，不然后续判断时会报错
            # Ex: str_flag = rs['STYLE'].loc[0]
            # print(rs['STYLE'].loc[[0]]) # 等价于 print(rs['STYLE'].[0:1]) 含索引列，返回的是一个

            df_owner.append(df_tmp)
    return df_owner


def df_html_bokeh(title, tooltips, source, plot_width=1300, plot_height=674, **kwargs):
    """
    :param title: HTML标题
    :param tooltips: Bokeh工具配置
    :param source: pd.read_csv ---> type: <class 'bokeh.models.sources.ColumnDataSource'>
    :param plot_width: 图表宽度
    :param plot_height: 图表高度
    :param kwargs: 其余图形配置参数

    :return: .html
    """
    # Ex:
    # tools = [
    #     ("FILE_SIZE", "@{y}{0.2f}MB"),
    #     ("HOUR", "@x"),
    #     ("FILE_STYLE", "@flag")
    # ]

    # HTML Figures Display Configuration
    p = figure(
        tooltips=tooltips,  # [("x", "$x"), ("y", "$y")],
        plot_width=plot_width,
        plot_height=plot_height,
        title=title,    # 'Gather Glyphs',
        x_axis_label=kwargs['x_axis_label'],    # 'HOUR',
        y_axis_label=kwargs['y_axis_label'],    # 'FILE_SIZE',
    )
    p.x_range.range_padding = p.y_range.range_padding

    if kwargs['color'] is None:
        p.line(kwargs, source=source)
        # p.line(x=kwargs['x'], y=kwargs['y'], legend=kwargs['legend'], line_width=kwargs['line_width'], source=source)
        p.circle(kwargs, size=10, source=source)
        # p.circle(x=kwargs['x'], y=kwargs['y'], legend=kwargs['legend'], size=10, source=source)
    else:
        p.line(x=kwargs['x'], y=kwargs['y'], legend=kwargs['legend'], line_width=kwargs['line_width'],
               color=kwargs['color'], source=source)
        p.circle(x=kwargs['x'], y=kwargs['y'], legend=kwargs['legend'],
                 size=kwargs['size'], fill_color=kwargs['fill_color'], line_color=kwargs['line_color'], source=source)
    return p


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
    # # sys.path.append(".")
    # print(sys.path)

    info_filter_1 = {'column_flag': 'STYLE',    # 1 筛选字段
                     'owner_flag': ('NSN_4G_PM', 'HW_4G_PM<OMC1>', 'HW_4G_PM<OMC2>',
                                    'NSN_4G_CM', 'HW_4G_CM<OMC1>', 'HW_4G_CM<OMC2>'),   # 2 筛选值
                     'owner_color': ('RED', 'ORANGE', 'SKYBLUE', 'GREEN', 'CHOCOLATE', 'BLUEVIOLET'),   # 3
                     'y': 'NOW'}  # 4 根据自己需要展示的Y轴字段名来配置

    df_list = df_preparation(data_input_path=bas_mail_conf.data_source, cols=bokeh_conf.cols,
                             cols_rename_dic=bokeh_conf.cols_rename_dic,
                             html_output_path=bokeh_conf.html_output_path,
                             info_filter=info_filter_1)

    for rs in df_list:
        print(rs.data)


