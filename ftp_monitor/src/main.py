# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:48
# @File: main.py
# @Usage: Main

from ftp_wc import *
from time import sleep

if __name__ == '__main__':
    for rs in ftp_conf.ftp_conf_dict:
        # print(rs)
        ftp = ftp_connect(host=rs['host'], port=rs['port'], usr=rs['usr'], passwd=rs['passwd'])
        if ftp == 1:
            # print(f'FinishStatus: User {rs["usr"]} Exception!', '\n'*2)
            print(f'FinishStatus: Exception!', '\n'*2)

        else:
            remote_path = rs['remotePath']
            # print(f'已配置的远程路径: {remote_path}')
            ftp = ftp_nlst(ftp, remote_path=remote_path)
            if ftp == 1:
                print(f'FinishStatus: Exception!', '\n' * 2)
            else:
                print(f'FinishStatus: Succeed!', '\n' * 2)

    input_word = input('Input any key to quit:')
