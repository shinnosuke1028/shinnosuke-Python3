# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2020/1/3 14:48
# @File: main.py
# @Usage: Drive

# CMD Mode
from class_email_daily_3 import *
import threading


if __name__ == '__main__':
    print('Thread', threading.current_thread().getName(), 'is Running. Time: %s' % date_f()[2])

    # mail_dict_combine = main_job()
    demo()

    print('Thread', threading.current_thread().getName(), 'End. Time: %s' % date_f()[2])
    key = input('Put any key to quit...')