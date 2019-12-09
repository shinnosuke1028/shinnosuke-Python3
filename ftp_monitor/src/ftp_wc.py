# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:13
# @File: ftp_wc.py
# @Usage: Generation for Remote FTP List

import ftplib
import os
import socket
import sys
# import csv

# Self Repo
# from src.func_demo.func_f import date_f
from src.conf.ftp_conf_bak import ftp_ip_dict, file_nlst_path, fileDict
from src.func_demo.Oracle2File import FileWR


def ftp_connect(host, usr, passwd, port=21, timeout=5):
    """

    :param host: remote FTP address
    :param usr: username for FTP
    :param passwd: password
    :param port: port <int>
    :param timeout: the timeout to set against the ftp socket(s)
    :return: <class 'ftplib.FTP'> or 1<num>

    """
    try:
        print(f'Current Connection Info: {host}:{port}/{usr}')
        ftp = ftplib.FTP()
        # ftp.encoding = 'utf-8'
        # print(type(ftp))
        ftp.connect(host, port, timeout)
    except socket.timeout as e:
        print('Status: Timed out during connection.')
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)
        return 1
        # raise OSError('FTP connect timed out!')
    except ConnectionRefusedError as e:
        print('Status: Login failed. Please check whether the remote address is normal.')
        print('------------------'*2, f'\nError Details:\n{e}')
        print('------------------'*2)
        return 1
    else:
        ftp.set_debuglevel(0)  # 打开调试级别2，显示详细信息
        try:
            ftp.login(usr, passwd)
            print(f'***Welcome Infomation: {ftp.welcome}***')  # 打印出欢迎信息
            print(f'Status: FTP User <{usr}> has connected to <{host} {port}>.')
            return ftp
        except ftplib.error_perm as e:
            print('Status: Login failed. Please check whether the login information is correct.')
            print('------------------'*2, f'\nError Details:\n{e}')
            print('------------------'*2)
            return 1
        except socket.timeout as e:
            print('Status: Time out during login.')
            print('------------------'*2, f'\nError Details:\n{str(e).title()}')
            print('------------------'*2)
            return 1


def ftp_nlst(ftp, remote_path):
    """

    :param ftp: <class 'ftplib.FTP'>
    :param remote_path: FTP file path
    :return: <class 'ftplib.FTP'>

    """
    # ftp = ftplib.FTP()
    print(f'***NLST***')

    # Local Path
    print(f'--Local script path: {os.getcwd()}')
    # Remote Path
    print(f'--Remote path: {remote_path}')
    try:
        n_lst_decode = []
        ftp.cwd(remote_path)
        # ftp.dir() # <class 'NoneType'>
        # print(f'ftp_dir: {type(ftp_dir)}')
        cur = ftp.pwd()
        print(f'Status: Successfully change dirName into {cur}.')
        try:
            n_lst = ftp.nlst()  # <class 'list'>
            for rs in n_lst:
                n_lst_decode.append(rs.encode('iso-8859-1').decode('gbk'))  # 解决Python3中文乱码  latin-1 ---> gbk/gb2312
            print(n_lst_decode)
            ftp.retrlines(cmd='LIST')
            return n_lst_decode

        except Exception as e:
            print('Status: Failed to obtain the file lists!')
            print('------------------' * 2, f'\nError Details:\n{e}')
            print('------------------' * 2)
            return 1

        # try:
        #     with open(output_file, 'w', newline='', encoding='UTF-8') as file_1:
        #         writer_csv = csv.writer(file_1)
        #         writer_csv.writerow([file_title])
        #         for row in n_lst:
        #             writer_csv.writerow([row])  # csv提供的写入方法可以按行写入list，无需按照对象一个个写入，效率更高
        #         # writer_csv.writerows([n_lst])
        #     return 0
        # except Exception as e:
        #     print('Status: 文件写入失败!')
        #     print('------------------' * 2, f'\nError Details:\n{e}')
        #     print('------------------' * 2)
        #     return 1

    except ftplib.all_errors as e:
        print('Status: Path switching failed!')
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)
        return 1
    finally:
        ftp.close()


def ftp_nlst_write(message_date, local_path, file_flag, file_title):
    """

    :param message_date: list input
    :param local_path:  local file path for out-put
    :param file_flag:   hint to distinguish between different flag_lists
    :param file_title:  file title
    :return file_title  <class 'list'> or 1

    """
    print(f'***NLST_WRITE***')
    # 清单
    local_path = file_nlst_path
    file_title = fileDict['LOCAL']['title']
    file_flag = fileDict['LOCAL']['flag']

    try:
        file = FileWR(local_file_path=local_path, title=file_title)
        file.file_write_f(message_date=message_date, job_flag=file_flag)
        return 0

    except Exception as e:
        print('Status: File write error!')
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)
        return 1


# if __name__ == '__main__':
