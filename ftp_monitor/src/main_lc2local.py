# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 20/1/15 15:10
# @File: main_lc2local.py
# @Usage: 浪潮笔记本数据传输至本地


# Self Repo
from src.ftp_wc import ftp_connect, ftp_nlst, ftp_download
from src.func_demo.func_f import date_f
from src.conf.ftp_conf_bak import ftp_ip_dict


if __name__ == '__main__':
    ip = '192.168.8.100'
    remote_path = f'/103/'
    local_path = f'./data/'
    # remote_file = f'{remote_path}'

    # 时间戳准备
    y = date_f()[3]['year']
    m = date_f()[3]['month']
    d = date_f()[3]['day']
    # h = date_f()[3]['hour']

    re_pattern = f'.*?{y}{m}{d}.*?csv$'
    file_name = '20200115_85_HW_CM.csv'


    print(ftp_ip_dict['LOCAL2']['host'])

    # if len(ftp_ip_dict['LOCAL2'][1]) == 0:
    #     ftp = ftp_connect(host=ftp_ip_dict['LOCAL2']['host'],)
    #
    #                 len(ftp_ip_dict['LOCAL2']) == 1:
    #     ftp = ftp_connect(host=ftp_ip_dict['LOCAL2'][0]['host'],)
    #     re_list = ftp_nlst(ftp,
    #                        remote_path=ftp_ip_dict['LOCAL2'][0]['remotePath'],
    #                        re_pattern=ftp_ip_dict['LOCAL2'][0]['re_pattern']
    #                        )
    #
    # else:
    #     ftp = ftp_connect(host=ftp_ip_dict['LOCAL2'][0]['host'],)
    #     re_list = ftp_nlst(ftp,
    #                        remote_path=ftp_ip_dict['LOCAL2'][0]['remotePath'],
    #                        re_pattern=ftp_ip_dict['LOCAL2'][0]['re_pattern']
    #                        )

    ftp = ftp_connect(host=ftp_ip_dict['LOCAL2']['host'],)
    #
    re_list = ftp_nlst(ftp,
                       remote_path=ftp_ip_dict['LOCAL2']['remotePath'],
                       re_pattern=ftp_ip_dict['LOCAL2']['re_pattern']
                       )  # <class 'list'>

    # ftp2 = ftp_connect(host=ftp_ip_dict['LOCAL2']['host'],)
    # ftp_download(ftp2, remote_path=remote_path, local_path=local_path, re_pattern=re_pattern)


    # import ftplib
    # ftp_tmp = ftp_connect(ip)
    # # ftp_tmp.connect(ip)
    # # ftp_tmp.login(None, None)
    # ftp_tmp.cwd(remote_path)
    # # fd = open(f'{local_path}{file_name}', 'wb')
    # with open (f'{local_path}{file_name}', 'wb') as f:
    #     ftp_tmp.retrbinary(f'RETR {file_name}', f.write)
    # # fd.close()
    # ftp_tmp.close()
