# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:48
# @File: main.py
# @Usage: Main

from src.ftp_wc import *
from src.func_demo.os_f import file_create


if __name__ == '__main__':
    # 测试用文件&文件夹生成，生产环境无需部署以下两步
    # file_create(d_path, 0, d_name)
    # file_create(f_path, 1, *file_name_list)

    ftp_result = []
    try:
        for rn_dict in ftp_ip_dict.keys():
            # 测试Limit
            if rn_dict == 'NSN_CM':
                # pprint.pprint(ftp_ip_dict[rs])
                print(f'{rn_dict}: {ftp_ip_dict[rn_dict]}')

                for rs in ftp_ip_dict[rn_dict]:
                    # print(rs)  # <class 'dict'>
                    # 这里后续可以引入多线程，同时统计当前采集服务器上的对应的所有IP
                    ftp = ftp_connect(host=rs['host'], port=rs['port'], usr=rs['usr'], passwd=rs['passwd'])
                    if ftp == 1:
                        # print(f'FinishStatus: User {rs["usr"]} Exception!', '\n'*2)
                        print(f'FinishStatus: Connect Exception!', '\n' * 2)
                    else:
                        remote_path = rs['remotePath']
                        # print(f'已配置的远程路径: {remote_path}')
                        ftp_reply = ftp_nlst(ftp, remote_path=remote_path, re_pattern=rs['re_pattern']) # <class 'list'>
                        if ftp_reply == 1:
                            print(f'FinishStatus: NLST Exception!', '\n' * 2)
                        else:
                            # print(type(ftp_reply))
                            ftp_reply.insert(0, rn_dict)
                            print(f'ftp_reply: {ftp_reply}')
                            ftp_result.extend([ftp_reply])
                            print(f'FinishStatus: Succeed!', '\n' * 2)

    except (KeyError, Exception) as e:
        print(f'FinishStatus: Exception!', '\n' * 2)
        print('------------------' * 2, f'\nError Details:\n{e}')
        print('------------------' * 2)

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

    # 清单采集
    # 这里可以引入多线程，同时统计多个服务器上的采集清单
    ftp_nlst_write(ftp_result, local_path=file_nlst_path, file_flag=fileDict['NSN_CM']['flag'], file_title=fileDict['NSN_CM']['title'])
    print(f'Final List Status: \n{ftp_result}')

    # 清单处理

    # 暂停
    # input_word = input('Input any key to quit: ')

