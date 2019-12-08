# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:48
# @File: main.py
# @Usage: Main

from src.ftp_wc import *
from time import sleep

if __name__ == '__main__':
    ftp_result = []
    for rs in ftp_ip_dict:
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
                try:
                    ftp_result.extend(ftp)
                    print(f'FinishStatus: Succeed!', '\n' * 2)
                except Exception as e:
                    print(f'FinishStatus: Exception!', '\n' * 2)
                    print('------------------' * 2, f'\nError Details:\n{e}')
                    print('------------------' * 2)

    print(f'Final List Status: \n{ftp_result}')
    input_word = input('Input any key to quit: ')


# if __name__ == '__main__':
#     key_map = []
#     for key in ftp_ip_dict.keys():
#         # print(f'key: {key}')
#         key_map.append(key)
#     # print(keys_map)
#
#     for key in key_map:
#         # print(ftp_ip_dict[key], '\n')
#         for rs in ftp_ip_dict[key]:
#             print(f'{key}: {rs}')
#         print('\n')
#
#         ftp = ftp_connect(host=rs['host'], port=rs['port'], usr=rs['usr'], passwd=rs['passwd'])
#         if ftp == 1:
#             # print(f'FinishStatus: User {rs["usr"]} Exception!', '\n'*2)
#             print(f'FinishStatus: Exception!', '\n'*2)
#
#         else:
#             remote_path = rs['remotePath']
#             # print(f'已配置的远程路径: {remote_path}')
#             ftp = ftp_nlst(ftp, remote_path=remote_path)
#             if ftp == 1:
#                 print(f'FinishStatus: Exception!', '\n' * 2)
#             else:
#                 print(f'FinishStatus: Succeed!', '\n' * 2)
#
#     input_word = input('Input any key to quit:')
