# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/10 15:11
# @File: os_f.py
# @Usage: Folder

"""

@OS Operation

"""
import os

# Self Repo
# from src.func_demo.func_f import date_f


def file_create(file_path, style, *args,):
    """

    :param file_path:   File path
    :param style:       1: File     0: Dictionary
    :param args:       File name <class 'list'>

    :return:

    """
    for rs in args:
        print(f'***OS_Example***')
        print(f'rs: {rs}')
        file = file_path + str(rs)
        print(f'Target path&name: {file}')
        if os.path.exists(file):
            if os.path.isdir(file):
                print(f'Status: {file} is an existing directory.\n')
                continue
            else:
                print(f'Status: {file} is an existing file.\n')
                continue
        elif style == 1:
            try:
                with open(file=file, mode='w', newline='', encoding='UTF-8') as f:
                    f.write(file)
                print(f'Status: File {rs} has been created. Path is {file}\n')
            except IOError as e:
                print('------------------' * 2, f'\nError Details:\n{e}')
                print('------------------' * 2)
                return 1
        else:
            try:
                os.mkdir(file)
                print(f'Status: Dictionary {rs} has been created. Path is {file}\n')
            except OSError as e:
                print('------------------' * 2, f'\nError Details:\n{e}')
                print('------------------' * 2)
                return 1


# # Demo
# if __name__ == '__main__':
#     # flag = os.path.exists(r'D:\Hadoop\PyFloder\ftp_monitor\src\data_output\20191210_FileList.csv')
#     # 文件名
#     file_name_1 = f'34G_output_{date_f()[1]}_tmp.csv'
#     file_name_2 = f'5G_output_{date_f()[0]}.xml.gz'
#     # 文件名列表
#     file_name_list = [file_name_1, file_name_2]
#     # 文件路径
#     # f_path = rf'../../../../../FTP/test/{date_f()[0]}/'    # D:\Hadoop\PyFloder\ftp_monitor\src\func_demo\
#     f_path = os.path.abspath(f'../../../../../FTP/test/{date_f()[0]}') + '\\'  # 表示当前所处项目的绝对路径：D:\Hadoop\PyFloder\bokeh_test
#
#     # 文件夹名
#     d_name = f'{date_f()[0]}'
#     # 文件夹路径
#     d_path = os.path.abspath(f'../../../../../FTP/test') + '\\'
#
#     file_create(d_path, 0, d_name)
#     file_create(f_path, 1, *file_name_list)


    # html_path_1 = os.path.abspath('.')  # 表示当前所处文件夹的绝对路径：D:\Hadoop\PyFloder\email_test\src\class_test
    # html_path_2 = os.path.abspath('../../../../../FTP/test')  # 表示当前所处项目的绝对路径：D:\Hadoop\PyFloder\bokeh_test
    # print(os.getcwd())   # 表示当前所处的文件夹的绝对路径
