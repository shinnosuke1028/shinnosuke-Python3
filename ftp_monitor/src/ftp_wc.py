# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:13
# @File: ftp_wc.py
# @Usage: Generation for Remote FTP List

import ftplib
import os
import socket
import re
from copy import copy
# import sys
# import csv

# Self Repo
# from src.func_demo.func_f import date_f
from src.conf.ftp_conf_bak import *
from src.func_demo.Oracle2File import *
# from src.ftp_wc import *
# from src.func_demo.os_f import file_create


def ftp_connect(host, usr=None, passwd=None, port=21, timeout=5):
    """

    :param host: remote FTP address
    :param usr: username for FTP
    :param passwd: password
    :param port: port <int>
    :param timeout: the timeout to set against the ftp socket(s)

    :return: <class 'ftplib.FTP'> or 1<num>

    Ex: ftp = ftp_connect(host=rs['host'], port=rs['port'], usr=rs['usr'], passwd=rs['passwd'])

    """
    try:
        print(f'Current Connection Info: {host}:{port}/{usr}')
        ftp = ftplib.FTP()
        # ftp.encoding = 'utf-8'
        # print(type(ftp))
        ftp.connect(host, port, timeout)
    except socket.timeout as e:
        print(f'Status: Host<{host}> timed out during connection.')
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)
        return 1
        # raise OSError('FTP connect timed out!')
    except ConnectionRefusedError as e:
        print('Status: Login failed. Please check whether the remote address is normal.')
        print('------------------'*2, f'\nError Details:\n{e}')
        print('------------------'*2)
        return 3
    else:
        ftp.set_debuglevel(0)  # 打开调试级别2，显示详细信息
        try:
            ftp.login(usr, passwd)
            # print(f'***Welcome Infomation: {ftp.welcome}***')  # 打印出欢迎信息
            print(f'Status: FTP User <{usr}> has connected to <{host} {port}>.')
            return ftp
        except ftplib.error_perm as e:
            print('Status: Login failed. Please check whether the login information is correct.')
            print('------------------'*2, f'\nError Details:\n{e}')
            print('------------------'*2)
            return 2
        except socket.timeout as e:
            print('Status: Time out during login.')
            print('------------------'*2, f'\nError Details:\n{str(e).title()}')
            print('------------------'*2)
            return 1


def ftp_nlst(ftp, remote_path, re_pattern):
    """

    :param ftp:             <class 'ftplib.FTP'>
    :param remote_path:     FTP file path
    :param re_pattern:      RE: Regular expression
    Ex: ftp_reply = ftp_nlst(ftp, remote_path=remote_path, re_pattern=f'.*.{y}{m}{d}.*.csv$')

    :return: <class 'list'> or Error 1

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
            # print(f'n_lst_decode: {n_lst_decode}')
            # ftp.retrlines(cmd='LIST', <function>)
            match_result = re_match(list_input=n_lst_decode, re_pattern=re_pattern)
            print(f'match_result: {match_result}')
            ftp.close()
            return match_result

        except Exception as e:
            print('Status: Failed to obtain the file lists!')
            print('------------------' * 2, f'\nError Details:\n{e}')
            print('------------------' * 2)
            ftp.close()
            return 1

    except ftplib.all_errors as e:
        print('Status: Path switching failed!')
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)
        ftp.close()
        return 1

    finally:
        ftp.close()


def re_match(list_input, re_pattern):
    """

    :param list_input:  <class: list>
    :param re_pattern:  RE

    :return:    返回正则匹配结果集 re.compile(re_pattern).findall() <class: list>

    """
    print(f'***RE***')
    try:
        # # 1
        #     match_result = [rs for rs in list_input if re.match(re_pattern, rs)]

        # # 2
        #     match_result = []
        #     for rs in list_input:
        #         if re.match(re_pattern, rs):
        #             match_result.append(rs)
        #             print(f'Status: File match successfully. Filename: {rs}')
        #         else:
        #             # print(f'Match failed. Filename: {rs}')
        #             print(f'Match failed.')
        #     print(match_result)
        #     return match_result

        # 3
        #     match_result = []
        #     p = re.compile(re_pattern)
        #     for rs in list_input:
        #         if p.findall(rs):
        #             match_result.append(rs)
        #             print(rs)

        p = re.compile(re_pattern)
        match_result = [rs for rs in list_input if p.findall(rs)]  # 别忘了，findall返回的是 <class 'list'>
        # print(match_result)
        return match_result

    except Exception as e:
        print('Status: RE failed!')
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)
        return 1


def main_job(key):
    """

    :param key: 采集标签，标签用来区分清单采集的厂家和内容

    :return: <class: dict>  [ftp_findall, error_counter, error_list]
    Ex:


    """
    ftp_result = []
    ftp_findall = []
    error_list = []
    # error_counter = 0
    # try:
    if key in ftp_ip_dict.keys():
        print(f'***MAIN***')
        for rn_dict in ftp_ip_dict.keys():
            # 测试Limit
            if rn_dict == key:  # 'NSN_CM':
                # pprint.pprint(ftp_ip_dict[rs])
                # print(f'{rn_dict}: {ftp_ip_dict[rn_dict]}')

                for rs in ftp_ip_dict[rn_dict]:
                    # print(rs)  # <class 'dict'>
                    # 这里后续可以引入多线程，同时统计当前采集服务器上的对应的所有IP
                    ftp = ftp_connect(host=rs['host'], port=rs['port'], usr=rs['usr'], passwd=rs['passwd'])
                    if ftp == 1:
                        # print(f'FinishStatus: User {rs["usr"]} Exception!', '\n'*2)
                        print(f'FinishStatus: {key}_{rs["host"]} Connect Exception!', '\n' * 2)
                        # error_counter += 1
                        error_list.append(rs['host'])
                        continue
                    else:
                        remote_path = rs['remotePath']
                        # print(f'已配置的远程路径: {remote_path}')
                        ftp_reply = ftp_nlst(ftp, remote_path=remote_path, re_pattern=rs['re_pattern']) # <class 'list'>
                        if ftp_reply == 1:
                            print(f'FinishStatus: {key}_{rs["host"]} NLST Exception!', '\n' * 2)
                            # error_counter += 1
                            error_list.append(rs['host'])
                            continue
                        else:
                            print(f'ftp_current: {ftp_reply}')
                            ftp_result.extend(ftp_reply)
                            del ftp_reply
                        print(f'ftp_result: {ftp_result}')
                        print(f'FinishStatus: {key}_{rs["host"]} Succeed!\n')

                # Final Result Gather
                print('***FINAL***')
                for rf in ftp_result:
                    ftp_findall.append([key, rf])
                for err in error_list:
                    ftp_findall.append([key, err])
                print(f'ftp_findall: {ftp_findall}')

        # Results filled in dict
        data_dict = dict(ftp_findall=ftp_findall, error_list=error_list)
        return data_dict
    # elif not ftp_findall:
    #     return error_counter
    else:
        print(f'{key}_{rs["host"]} Error FTP Key in ftp_conf...')

    # except (KeyError, Exception) as e:
    #     print(f'FinishStatus: Exception!')
    #     print('------------------' * 2, f'\nError Details:\n{e}')
    #     print('------------------' * 2)
    #     return 1

    # 20191225-Demo1
    #     for rs in ftp_ip_dict:
    #         # print(rs)
    #         ftp = ftp_connect(host=rs['host'], port=rs['port'], usr=rs['usr'], passwd=rs['passwd'])
    #         if ftp == 1:
    #             # print(f'FinishStatus: User {rs["usr"]} Exception!', '\n'*2)
    #             print(f'FinishStatus: Connect Exception!', '\n'*2)
    #         else:
    #             remote_path = rs['remotePath']
    #             # print(f'已配置的远程路径: {remote_path}')
    #             ftp_reply = ftp_nlst(ftp, remote_path=remote_path, re_pattern=rs['re_rule'])
    #             if ftp_reply == 1:
    #                 print(f'FinishStatus: NLST Exception!', '\n' * 2)
    #             else:
    #                 # print(ftp_reply)
    #                 ftp_result.extend(ftp_reply)
    #                 print(f'FinishStatus: Succeed!', '\n' * 2)
    #                 # try:
    #                 #     ftp_result.extend(ftp_reply)
    #                 #     print(f'FinishStatus: Succeed!', '\n' * 2)
    #                 #     # ftp.quit() # 获取返回值后关闭服务
    #                 #
    #                 # except Exception as e:
    #                 #     print(f'FinishStatus: Exception!', '\n' * 2)
    #                 #     print('------------------' * 2, f'\nError Details:\n{e}')
    #                 #     print('------------------' * 2)
    #
    # except Exception as e:
    #     print(f'FinishStatus: Exception!', '\n' * 2)
    #     print('------------------' * 2, f'\nError Details:\n{e}')
    #     print('------------------' * 2)

    # print(type(ftp))
    # ftp.quit()
    # re_match(ftp_result, ftp_ip_dict[0]['re_rule'])


def ftp_nlst_write(message_date, local_path, file_flag, file_title):
    """

    :param message_date: list input
    :param local_path:  local file path for out-put
    :param file_flag:   hint to distinguish between different flag_lists
    :param file_title:  file title

    :return 0(file output) or 1

    """

    print(f'***NLST_WRITE***')
    # 清单
    # local_path = file_nlst_path
    # file_title = 'LOCAL'    # fileDict['LOCAL']['title']
    # file_flag = fileDict['HW_CM']['flag']

    try:
        file = FileWR(local_file_path=local_path, title=file_title)
        flag = file.file_write_f(message_date=message_date, job_flag=file_flag)
        return flag

    # 方法2:
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

    except (IOError, OSError, Exception) as e:
        print('Status: File write error!')
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)
        return 1


def ftp_download(ftp, remote_path, local_path, re_pattern):
    """

    :param ftp:  FTP连接池
    :param remote_path: 远程文件路径（不含文件名）
    :param local_path:  本地保存路径（含文件名）

    :return:  0 or 1

    """

    # ftp = ftplib.FTP()
    ftp_remain = copy(ftp)
    try:
        # buf_size = 1024

        print(f'***Download***')
        # ftp.set_debuglevel(0)

        ftp.cwd(remote_path)
        re_list = ftp_nlst(ftp, remote_path=remote_path, re_pattern=re_pattern)  # <class 'list'>
        print(f're_list: {re_list}')

        # ftp = ftp_connect(host=rs['host'], port=rs['port'], usr=rs['usr'], passwd=rs['passwd'])

        # xx = ftp_connect('192.168.8.100')
        # xx.cwd(remote_path)
        ftp_remain.cwd(remote_path)
        for rs in re_list:
            with open(f'{local_path}{rs}', 'wb') as f:
                print(f'current: {local_path}{rs}')
                ftp.retrbinary(f'RETR {rs}', f.write) # 别加blocksize=1024
        return 0

    except Exception as e:
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)
        return 1
    finally:
        ftp_remain.close()


# if __name__ == '__main__':
#     pass
