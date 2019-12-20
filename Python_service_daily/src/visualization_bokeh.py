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
import copy

from src.func_test.func_f import date_f
from src.conf import bas_mail_conf

data_path = r'../data/'
data_path = data_path + date_f(-1)[0] + '_GATHER.csv'
# print(data_path)
# print(sys.path)

# data = pd.read_csv(data_path, encoding='ANSI')
data = pd.read_csv(data_path,
                   usecols=['DATA_STYLE', 'HOUR', 'NORMAL_FILE_SIZE/MB', 'NOW_SIZE/MB'],
                   # index_col='HOUR',
                   # header=None,
                   # sep=',',
                   index_col=['HOUR']   # 这里设置列索引后，后续不需要重复设置 # 1
                   )

# print(data)
# print(type(data), id(data))
dic_col = {'DATA_STYLE': 'STYLE', 'NORMAL_FILE_SIZE/MB': 'NORMAL', 'NOW_SIZE/MB': 'NOW'}
data_rename = data.rename(columns=dic_col)  # 2
# print(data_rename)
# print(id(data_rename))
del data  # <------

# 按照列值筛选
# data_DataFrame = data_rename.loc[data_rename['STYLE'] == 'NSN_4G_PM'].set_index('HOUR')
df_nsn = data_rename[data_rename['STYLE'] == 'NSN_4G_PM']   # .set_index('HOUR') # 1
df_hw_pm1 = data_rename[data_rename['STYLE'] == 'HW_4G_PM<OMC1>']
df_hw_pm2 = data_rename[data_rename['STYLE'] == 'HW_4G_PM<OMC2>']
df_nsn_cm = data_rename[data_rename['STYLE'] == 'NSN_4G_CM']
df_hw_cm1 = data_rename[data_rename['STYLE'] == 'HW_4G_CM<OMC1>']
df_hw_cm2 = data_rename[data_rename['STYLE'] == 'HW_4G_CM<OMC2>']
# print(id(data_DataFrame))
# print(data_rename)
del data_rename

# 时间轴截取
col_hour = df_nsn.index

# cols = {u'DATA_STYLE': '类型', u'NORMAL_FILE_SIZE/MB': '正常', u'NOW_SIZE/MB': '当前'}
# data_DataFrame_2 = data_DataFrame.copy()
# 不进行Copy时，会报链式赋值Warning，即更改的对象指向的其实是原始内存地址保存的变量，这里指向的是data
# 但第二次实验时，又不报错了，原因是对先前的data进行了内存释放  ------>
# 要么释放掉最原始的数据来源，即箭头指向处；要么在生成 DataFrame 时就进行浅拷贝,规避链式赋值   # 2
# print(id(data_DataFrame_2))
# data_DataFrame.rename(columns=cols, inplace=True) # 第二个参数使用时有疑问？？？


# Visual
def visual_html(local_file_path):
    output_file(local_file_path)

    # 在这里封装各线图源数据
    source = ColumnDataSource(data=dict(
        x=col_hour,
        y=df_nsn['NOW'],
        nsn_pm_flag=df_nsn['STYLE'],
    ))

    tools = [
        ("FILE_SIZE", "@{y}{0.2f}MB"),
        ("HOUR", "$index"),
        ("FILE_STYLE", "@nsn_pm_flag")
    ]

    p = figure(
                tooltips=tools,  # [("x", "$x"), ("y", "$y")],
                plot_width=1300,
                plot_height=674,
                title='Example Glyphs',
                x_axis_label='HOUR',
                y_axis_label='FILE_SIZE/MB',
               )
    p.x_range.range_padding = p.y_range.range_padding
    # p.y_range.end = 2200
    # p.y_range.max_interval = 100
    p.line(x='x', y='y', legend='NSN_4G_PM', line_width=3, source=source)
    p.circle(x='x', y='y', legend='NSN_4G_PM', size=10, source=source)

    # p.line(x=col_hour, y=df_nsn['NORMAL'], legend='NSN_4G_PM', line_width=3)
    # p.circle(x=col_hour, y=df_nsn['NORMAL'], legend='NSN_4G_PM', size=10)

    p.line(x=col_hour, y=df_hw_pm1['NORMAL'], legend='HW_4G_PM_OMC1', line_width=3, color='ORANGE')
    p.circle(x=col_hour, y=df_hw_pm1['NORMAL'], legend='HW_4G_PM_OMC1', line_color='ORANGE', fill_color='ORANGE', size=10)

    p.line(x=col_hour, y=df_hw_pm2['NORMAL'], legend='HW_4G_PM_OMC2', line_width=3, color='RED')
    p.circle(x=col_hour, y=df_hw_pm2['NORMAL'], legend='HW_4G_PM_OMC2', line_color='RED', fill_color='RED', size=10)

    # p.line(x=-1, y=df_nsn_cm['NORMAL'], legend='NSN_4G_CM', line_width=3, color='MAROON')
    p.circle_cross(x=-1, y=df_nsn_cm['NORMAL'], legend='NSN_4G_CM', line_color='MAROON', fill_color='MAROON', size=15)

    # p.line(x=[0, 5], y=df_hw_cm1['NORMAL'], legend='HW_4G_CM_OMC1', line_width=3, color='magenta')
    p.circle_cross(x=[0, 5], y=df_hw_cm1['NORMAL'], legend='HW_4G_CM_OMC1', line_color='magenta', fill_color='magenta', size=15)

    # p.line(x=-1, y=df_hw_cm2['NORMAL'], legend='HW_4G_CM_OMC2', line_width=3, color='LIGHTSALMON')
    p.circle_cross(x=-1, y=df_hw_cm2['NORMAL'], legend='HW_4G_CM_OMC2', line_color='LIGHTSALMON', fill_color='LIGHTSALMON', size=15)

    show(p)


def html_line(local_file_path, title, plot_width=1300, plot_height=674, **kwargs):
    output_file(local_file_path)

    tools = [
        ("FILE_SIZE", "@{y}{0.2f}MB"),
        ("HOUR", "$index"),
    ]

    fig = figure(
        tooltips=tools,  # [("x", "$x"), ("y", "$y")],
        plot_width=plot_width,
        plot_height=plot_height,
        title=title,    # 'Gather Glyphs',
        x_axis_label=kwargs['x_axis_label'],    # 'HOUR',
        y_axis_label=kwargs['y_axis_label'],    # 'FILE_SIZE',
    )
    fig.x_range.range_padding = fig.y_range.range_padding

    if kwargs['color'] is None:
        fig.line(x=kwargs['x'], y=kwargs['y'], legend=kwargs['legend'], line_width=kwargs['line_width'])
        fig.circle(x=kwargs['x'], y=kwargs['y'], legend=kwargs['legend'], size=10)
    else:
        fig.line(x=kwargs['x'], y=kwargs['y'], legend=kwargs['legend'], line_width=kwargs['line_width'],
                 color=kwargs['color'])
        fig.circle(x=kwargs['x'], y=kwargs['y'], legend=kwargs['legend'],
                   size=kwargs['size'], fill_color=kwargs['fill_color'], line_color=kwargs['line_color'])

    return fig


if __name__ == '__main__':
    html_path = bas_mail_conf.mail_file_path_class + '\\' + 'log_lines.html'
    print(html_path)
    visual_html(html_path)

    htmlDict = {
        'x_axis_label': 'HOUR',
        'y_axis_label': 'FILE_SIZE/MB',
        'x': col_hour,
        'y': df_nsn['NORMAL'],
        'legend': 'NSN_4G_PM',
        'line_width': 3,
        'color': None,
        'size': None,
        'fill_color': None,
        'line_color': None
    }
    htmlDict_hw1 = {
        'x_axis_label': 'HOUR',
        'y_axis_label': 'FILE_SIZE/MB',
        'x': col_hour,
        'y': df_nsn['NORMAL'],
        'legend': 'HW_4G_PM_OMC1',
        'line_width': 3,
        'color': 'RED',
        'size': 10,
        'fill_color': 'RED',
        'line_color': 'RED'
    }
    htmlDict_hw2 = {
        'x_axis_label': 'HOUR',
        'y_axis_label': 'FILE_SIZE/MB',
        'x': col_hour,
        'y': df_nsn['NORMAL'],
        'legend': 'HW_4G_PM_OMC2',
        'line_width': 3,
        'color': 'ORANGE',
        'size': 10,
        'fill_color': 'ORANGE',
        'line_color': 'ORANGE'
    }

    # 循环加载线条
    # p = html_line(local_file_path=html_path, title='Gather HTML Glyphs', **htmlDict)
    # show(p)
