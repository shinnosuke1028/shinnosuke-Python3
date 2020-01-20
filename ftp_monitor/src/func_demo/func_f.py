# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/9/5 8:08
# @File: class_email_daily.py

import datetime
import os
import shutil
from re import compile as re_compile
from sys import exc_info

# import src.conf.bas_insert_conf


def date_f(daysdelta=0, hoursdelta=0):
    """
    :param timedelta: Day Intervals
    :param hoursdelta: Hour Intervals
    :return: date <class:tuple>
            Ex: ('20191205', '2019120522', '20191205 22:40:51',
                 {'year': '2019', 'month': '12', 'day': '05', 'hour': '22'}, 'src.func_demo.func_f')
    """
    # date = []
    # date_str = datetime.datetime.now().strftime('%Y%m%d')

    date_str = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%Y%m%d')
    date_str_h = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%Y%m%d%H')
    date_str_s = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%Y%m%d %H:%M:%S')

    # 各维度时间戳段拆分
    year = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%Y')
    month = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%m')
    day = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%d')
    hour = (datetime.datetime.now() + datetime.timedelta(days=daysdelta, hours=hoursdelta)).strftime('%H')
    date_dict = dict(year=year, month=month, day=day, hour=hour)

    # 所有已知时间格式组合
    date = (date_str, date_str_h, date_str_s, date_dict, __name__)
    return date


def file_rm_all(local_file_path):
    """
    :param local_file_path: 待检索文件夹
    :return:
    """
    for path in os.walk(local_file_path):
        print(path) # (<current path>, <dictionary in current path>, ['34G_output_2019121022_tmp.csv', '5G_output_20191210.xml.gz'])
        for file_cur in path[-1]:
            print(file_cur)   # ['34G_output_2019121022_tmp.csv', '5G_output_20191210.xml.gz']
            if file_cur.split('_')[0] != date_f(0)[0]:
                os.remove(path[0] + '\\' + file_cur)
                # pass
            else:
                pass


def file_rm_single(path2load, re_pattern):
    """
    @暂不支持清理指定路径下的子文件夹
    :param path2load: 文件待清理路径（绝对路径）
    :param re_pattern: 清理正则

    :return: <class list> 已清理的对象清单

    """
    match_result_all = {}
    try:
        print(f'***FILE DROP WITH LOCATION***')
        # for rp in path2load:
        print(f'length of path2load: {len(path2load)}')
        for rp in path2load:
            print(f'rp: {rp}')
            for path in os.walk(rp):
                print(f'path2load: {path}')
                # for file_cur in path[-1]:
                    # print(f'file_cur: {file_cur}')    # 打印待清理路径下的所有文件，不包含待清理路径下的子文件夹
                p = re_compile(re_pattern)
                match_result = [rs for rs in path[-1] if p.findall(rs)]  # 别忘了，findall返回的是 <class 'list'>
                print(f'match_result: {match_result}')
                print('------------------' * 2, '\n')
                # match_result_all.extend(match_result)
                match_result_all[rp]=match_result
        print('Status: OK.')
        print(f'File drop list: {match_result_all}')

    except (OSError, Exception) as e:
        print('Status: File write error!')
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)
        return 1
    finally:
        return match_result_all


def file_copy_f(path_in, path_target):
    """

    :param path_in: 源文件路径（含文件名）
    :param path_target: 目标文件路径

    :return: None

    """
    # adding exception handling
    try:
        print('Status: File copy.')
        shutil.copy(path_in, path_target)
        exit(0)
    except IOError as e:
        print('Status: Unable to copy file. %s' % e)
        exit(1)
    except:
        print("Status: Unexpected errors:", exc_info())
        exit(1)


# if __name__ == '__main__':
#     # file_rm_f(r'D:\FTP\test\20191210')
#     print(date_f())
#     # move(3,'A','B','C')

