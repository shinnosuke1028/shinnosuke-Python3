# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/9/19 0:25
# @File: py
"""
邮件发送配置
"""
from func_demo.func_f import date_f
y = date_f()[3]['year']
m = date_f()[3]['month']
d = date_f()[3]['day']
h = date_f()[3]['hour']


# # 数据库连接信息
# connect_info = 'LRNOP/Inspur*()890@192.168.62.53:1521/SHIRNOP'

# 本地文件路径
mail_file_path = r'D:\FTP\Mail'
# mail_file_path_class = r'D:\IdeaProjects\Python_service_daily\data'
mail_file_path_class = r'./data_output/'

# D:\\IdeaProjects\\Python_service_daily\\src\\class_test

# 报表文件标题
file_title_pkg = '执行时间', '执行包', '执行次数', '最终数据结果'  # 注意，字符串元素内逗号要用英文类型，不可使用正文逗号，不然无法按照逗号分割，填入csv
file_title_job = ['任务编号', '失败次数', '下次执行时间', '执行间隔', '执行内容']
file_title_gather = 'S_DATE', 'DATA_STYLE', 'HOUR', '包内数据时间', 'NORMAL_FILE_NUM', 'NOW_NUM', 'BEF_NUM', 'NORMAL_FILE_SIZE/MB', 'NOW_SIZE/MB', 'BEF_SIZE/MB', 'FILE_NUM<今-昨>', 'FILE_SIZE<今-昨>/MB', 'FILE_NUM_STATUS', 'FILE_SIZE_STATUS', 'PATH_NAME'
file_title_scheduler = 'LOG时间', '任务名', '任务状态', '耗时', '下次执行时间', '执行间隔', '任务激活状态', '上次执行时间', '包内容'

titleDict = {
    'CONF_JOB': file_title_job,
    'CONF_PKG': file_title_pkg,
    'CONF_GATHER': file_title_gather,
    'CONF_SCHEDULER': file_title_scheduler
}

# Re
file_pattern = rf'.*{y}{m}{d}.*?.csv$'

# 待入库文件名
# 邮件的文件名自动生成
mail_file_name = ''
# 若无文件名，则需拼接路径+文件名
# 入库路径+文件名
mail_csv_file = mail_file_path + '\\' + mail_file_name

# 收件人配置
receivers = ['guohaoran@inspur.com', 'yangqidong@inspur.com']    # '89304594@qq.com'
# receivers = ['717648387@qq.com', 'guohaoran@inspur.com']


# bokeh配置
# data source
data_origin = '../data/'
data_path = './data/'
html_output_path = './data_output/'
data_source = data_path + date_f()[0] + '_GATHER.csv'
# target
# bokeh_target = data_path
# 指标
owner_flag = ('NSN_4G_PM', 'HW_4G_PM<OMC1>', 'HW_4G_PM<OMC2>', 'NSN_4G_CM', 'HW_4G_CM<OMC1>', 'HW_4G_CM<OMC2>')
# 线条色彩(数量对应指标数量)
owner_color = ('RED', 'ORANGE', 'SKYBLUE', 'GREEN', 'CHOCOLATE', 'BLUEVIOLET')

# print(bokeh_data_source)
