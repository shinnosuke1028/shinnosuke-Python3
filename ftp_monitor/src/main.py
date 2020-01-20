# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 11:48
# @File: main.py
# @Usage: Main

# Self Repo
from src.ftp_wc import *
from src.func_demo.os_f import file_create
from concurrent.futures import ThreadPoolExecutor, as_completed


def ftp_job(flag, local_path, file_flag, file_title ):
    """
    :param flag:        采集标签，标签用来区分清单采集的厂家和内容
    :param local_path:  清单输出路径
    :param file_flag:   hint to distinguish between different flag_lists
    :param file_title:  file title

    Ex:
        (main_job('HW_CM'), local_path=file_nlst_path, file_flag=fileDict['HW_CM']['flag'], file_title=fileDict['HW_CM']['title'])

        # 清单标签,和采集清单一一对应
        fileDict = {
            'LOCAL':{
                'title': ('flag', 'LOCAL'),
                'flag': 'FileList'
            },
            'HW_CM':{
                'title': ('flag', '85_HW_CM'),
                'flag': '85_HW_CM'
            },
            'HW_CM_NSA': {
                'title': ('flag', '85_HW_CM_NSA'),
                'flag': '85_HW_CM'
            },
            'NSN_CM': {
                'title': ('flag', '86_NSN_CM'),# '86_NSN_CM',
                'flag': '86_NSN_CM'
            },
        }

    :return:    counter: 0 or 1
    """
    res_list = main_job(flag)
    print(f'res_list: {res_list}')

    ftp_nlst_write(res_list["ftp_findall"], local_path=local_path, file_flag=file_flag, file_title=file_title)
    # return res_list["error_list"]


if __name__ == '__main__':
    # 测试用文件&文件夹生成，生产环境无需部署以下两步
    # file_create(d_path, 0, d_name)
    # file_create(f_path, 1, *file_name_list)
    print('------------------'*2)

    # 方法1 传统线程池方法
    # threads = []
    # # t= None
    # # 线程初始化
    # for rs in fileDict.keys():
    #     # print(rs)
    #     # print('%s配置:' % rs, sqlConf[rs])
    #     # print('%sTITLE配置:' % rs, fileTitleJob[rs])
    #     if rs == 'LOCAL':   # 'NSN_CM':
    #         t = MyThread(func=ftp_job, args=(rs, file_nlst_path, fileDict[rs]['flag'], fileDict[rs]['title']))
    #         # print(t.getName(), '\n')
    #         threads.append(t)
    #
    # # 线程批量启动
    # for rt in threads:
    #     rt.start()
    #     # print(f'Final List Status: \n{rt.get_result()}')
    #
    # for rt in threads:
    #     rt.join()
    #
    # # 清单采集
    # # 这里可以引入多线程，同时统计多个服务器上的采集清单
    # # ftp_result = main_job('HW_CM')
    # # ftp_nlst_write(ftp_result, local_path=file_nlst_path, file_flag=fileDict['HW_CM']['flag'], file_title=fileDict['HW_CM']['title'])
    # #
    # # ftp_result = main_job('HW_CM_NSA')
    # # ftp_nlst_write(ftp_result, local_path=file_nlst_path, file_flag=fileDict['HW_CM_NSA']['flag'], file_title=fileDict['HW_CM_NSA']['title'])
    # #
    # # ftp_result = main_job('NSN_CM')
    # # ftp_nlst_write(ftp_result, local_path=file_nlst_path, file_flag=fileDict['NSN_CM']['flag'], file_title=fileDict['NSN_CM']['title'])
    #
    # # 清单处理
    #
    # # 暂停
    # # input_word = input('Input any key to quit: ')


    # ######
    # all_task = []
    # # 方法3 ThreadPoolExecutor
    # # 需要打印时替换方法2
    # executor = ThreadPoolExecutor(max_workers=5)
    # for rs in fileDict.keys():
    #     # if rs in ['LOCAL', 'HW_CM_NSA']:
    #     if rs in ['LOCAL']:
    #         future = executor.submit(ftp_job, rs, file_nlst_path, fileDict[rs]['flag'], fileDict[rs]['title'])
    #         all_task.append(future)
    #         print(f'Future: {future}\n')
    #
    # ######
    # # # 方法2 ThreadPoolExecutor
    # # executor = ThreadPoolExecutor(max_workers=5)
    # # all_task = [executor.submit(ftp_job, rs, file_nlst_path, fileDict[rs]['flag'], fileDict[rs]['title'])
    # #             for rs in fileDict.keys() if rs in ['LOCAL', 'HW_CM_NSA'] ]
    #
    # A = None
    # for future in as_completed(all_task):
    #     if not future.result():
    #         print(f'Future: {future}, OK')
    #     else:
    #         print(f'Future: {future}, Error: {future.result()}')
    # ######

    ######
    # 方法4
    from multiprocessing.dummy import Pool as ThreadPool

    list_host = [rs['host'] for rs in ftp_ip_dict['LOCAL']]
    print(list_host)

    list_usr = [rs['usr'] for rs in ftp_ip_dict['LOCAL']]
    print(list_usr)

    list_passwd = [rs['passwd'] for rs in ftp_ip_dict['LOCAL']]
    print(list_passwd)

    list_port = [rs['port'] for rs in ftp_ip_dict['LOCAL']]
    print(list_port)

    list_timeout = [15 for rs in ftp_ip_dict['LOCAL']]
    print(list_timeout, '\n')




    # host, usr, passwd, port = 21, timeout = 5

    pool = ThreadPool(4)
    # results = pool.starmap(ftp_connect, [('192.168.73.1', '1320964752@qq.com', 'ghr921028', 21, 15),('192.168.73.2', '1320964752@qq.com', 'ghr921028', 21, 15)])
    # print(results)


    # 下面的两种配置模式，后者为.py配置文件，在 ftp_conf_bak.py 中
    pool.starmap(ftp_job,
                 [
                     # ('LOCAL', './data_output/', 'LOCAL', ('flag', 'LOCAL')),
                     # ('LOCAL2', './data_output/', 'LOCAL2', ('flag', 'LOCAL2')),
                     ('NSN_CM', './data_output/', fileDict['NSN_CM']['flag'], fileDict['NSN_CM']['title']),
                     # ('HW_PM_3G', './data_output/', 'HW_PM_3G', ('flag', 'HW_PM_3G')),
                     # ('HW_CM', './data_output/', fileDict['HW_CM']['flag'], fileDict['HW_CM']['title']),
                     ('HW_CM_NSA', './data_output/', fileDict['HW_CM_NSA']['flag'], fileDict['HW_CM_NSA']['title']),
                 ]

                 )



    # 以下配置个数带来的BUG,键值对内的值为tuple,但tuple内只有一个字典时,遍历方法会索引错误
    # key: ({},{})
    # key: ({})
    # for rs in ftp_ip_dict['LOCAL']:
    #     print(rs)
    #
    # for rs in ftp_ip_dict['LOCAL2']:
    #     print(rs)
    #
    # print(len(ftp_ip_dict['LOCAL']))
    #
    # print(len(ftp_ip_dict['LOCAL2']))

