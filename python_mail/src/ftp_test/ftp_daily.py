# -*- coding:utf-8 -*-
import ftplib
# import time,tarfile,os
import socket

from ftplib import FTP
from src.func_test.func_f import date_f


def ftpcon_f(host, port, usr, passwd):
    try:
        ftp = FTP()
        # 打开调试级别2，显示详细信息
        ftp.set_debuglevel(2)
        ftp.connect(host, port)
        ftp.login(usr, passwd)
        print(ftp.getwelcome())                     # 打印出欢迎信息
        return ftp
    except(socket.error, socket.gaierror):
        print('FTP登陆失败，请检查主机号、用户名和密码是否正确.')
    print('已连接到：%s' % host)


def ftpdownload_f(remotepath, filename):
    # ftp = FTP()
    ftp.cwd(remotepath)                             # 更改远程目录
    blocksize = 4096
    try:
        file_handle = open(filename,'wb').write()   # 待下载文件：写入本地文件的文件名
    except ftplib.error_perm as e:
        print(e)
        print('数据下载失败...')
        return False
    else:
        ftp.storbinary('STOR' + filename, file_handle, blocksize)
        ftp.set_debuglevel(0)
        ftp.close()
        print('数据下载完成...')
    return True


def ftpupload_f(remotepath, filename):
    # ftp = FTP()
    ftp.cwd(remotepath)                             # 更改远程目录
    blocksize = 4096
    file_handle = open(filename,'rb')               # 待上传文件：读本地文件的文件名
    try:
        ftp.storbinary('RETR'+ filename, file_handle, blocksize)
        ftp.set_debuglevel(0)
        ftp.close()
        print('本地数据上传完成...')
    except ftplib.error_perm as e:
        print(e)
        print('本地数据上传失败...')
        return False
    return True


def main_job_f():
    # host = '192.168.62.85'
    # port = '21'                                   # 21：用于连接 20：用于传输
    # usr = 'meatrnop'
    # passwd = 'Inspur*()890'                       # r:防止转义
    # filename = r'D:\ftp\ftp_test_file.csv'
    # remotepath = '/usr2/lrnop'
    # ftpcon_f(host,port,usr,passwd)
    # ftpdownload_f(remotepath,filename)
    date_f()


if __name__ == '__main__':
    main_job_f()
