# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 18:07
# @File: Oracle2File.py
# @Usage:

import sys
import math
# from os.path import abspath, join, dirname

import cx_Oracle
import csv
import smtplib
# import numpy as np
import threading

from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart

from time import sleep, ctime, time
# from tqdm import tqdm, trange

# 配置
# from src.func_test.func_f import date_f
# from conf import bas_mail_conf
# from conf import sql_conf
# from conf import bas_insert_conf

# sys.path.insert(0, join(abspath(dirname(__file__)), '../func_test/'))
sys.path.append('./func_test')
from func_demo.func_f import date_f


class OracleExecution(object):
    # def __init__(self, sql=None, connect=None, check_style=None):
    def __init__(self):
        self.__connect = None
        self.__sql = None
        self.__check_style = None
        # self.message_str = ''
        self.rs = []
        self.message = ''
        self.message_data = []
        self.db = None

    # # 装饰器用法1：
    # @property
    # def conf_f(self):
    #     return self.__connect, self.__sql, self.__check_style
    #
    # @conf_f.setter
    # def conf_f(self, value):
    #     self.__connect = value[0]
    #     self.__sql = value[1]
    #     self.__check_style = value[2]

    # 装饰器用法2：<1等价于2>
    def get_conf_f(self):
        return self.__connect, self.__sql, self.__check_style

    def set_conf_f(self, value):
        self.__connect = value[0]
        self.__sql = value[1]
        self.__check_style = value[2]

    conf_f = property(get_conf_f, set_conf_f)

    def connect_f(self):
        try:
            self.db = cx_Oracle.connect(self.__connect)
        except Exception as e:
            print(e)
        finally:
            return self.db

    def execute_f(self):
        # db = cx_oracle.connect(self.username + "/" + self.password + "@" +
        try:
            cur = self.db.cursor().execute(self.__sql)
        except Exception as e:
            print(e)
            self.db.rollback()
        else:
            self.rs = cur.fetchall()
            # print(self.rs)    # [] list类型
            if self.__check_style == 'JOB':
                data_flag = 0
                data_flag_bad = None
                i = 0
                for r1 in self.rs:
                    if i == 0:
                        data_flag = r1[1]
                        if data_flag != 0:
                            data_flag_bad = r1[0]
                        else:
                            continue
                    elif r1[1] is not None:
                        data_flag = data_flag + r1[1]
                        if data_flag != 0:
                            data_flag_bad = data_flag_bad + ',' + r1[0]
                            # 这里也可以直接写成 data_flag_bad = data_flag_bad, r1[0]
                        else:
                            continue
                    else:
                        data_flag = data_flag + 0
                        if data_flag != 0:
                            data_flag_bad = data_flag_bad + ',' + r1[0]
                        else:
                            continue
                    i += 1
                if data_flag != 0:
                    self.message = '数据库脚本或定时任务异常,请及时核查.报错任务号：' + str(data_flag_bad)
                    # print(data_flag_bad)
                else:
                    self.message = '数据库脚本正常执行,详细监测日志请查看附件.'
                print('<' + date_f(0)[1] + '> : ' + self.message)
            else:
                self.message = '定时邮件任务完成，详细信息请看附件.'
                print('<' + date_f(0)[1] + '> : ' + self.message)
            cur.close()
        finally:
            self.db.close()
            return self.message

    def execute_split_f(self):
        # message_data_all = []
        # message_data = []
        for r1 in self.rs:
            # 如果写成 str(r1), list内的行对象list会变成字符串：list [(),()] -> ["()","()"]
            # 写入时会把每一组对象当成一个个完整的字符串，即一个字母一个数字进一个单元格
            # 要写入.csv的文件，按行转换成最基本的list即可，一行一个list对象<即元组tuple>：list [(),()]；或list嵌套list：[[],[]]
            # 无需转换为str再拼接为数组
            try:
                # print(r1)
                # print(str(r1))
                self.message_data.append(r1)
                # print(self.message_data)
                # print(self.message_data[0][0])  # [(a,3),(b,4)] -> [0][1]='3'
            except Exception as e:
                print(e)
        return self.message_data


class FileWR(OracleExecution):
    # 初始化子类属性时，需带上父类属性，这种需全部初始化父类属性的写法，用在：调用含父类属性的父类方法时，不然实例化时，无法顺利调用父类方法
    # 调用不含父类属性的父类方法时，可以直接用：super(<子类名>, self).__init__()完成父类初始化
    # def __init__(self, local_file_path=None, title=None, connect=None, sql=None, check_style=None):
    def __init__(self, local_file_path=None, title=None):
        # super(FileWR, self).__init__(connect, sql, check_style)   # 初始化父类属性
        # super(FileWR, self).__init__()
        super().__init__()  # 简写
        # 初始化父类属性，父类属性有默认赋值时，在子类初始化时可以不再指明具体参数
        # 后续实例化时，若子类想调用父类属性，则可以直接调用父类属性并赋值；或写成注释行部分的形式去初始化父类属性，在初始化子类时一并加入父类属性
        self.local_file_path = local_file_path
        self.title = title
        # self.message_data = message_date
        self.file_name = ''  # 可以不定义在初始化内，只作为类的私有属性

    def file_write_f(self, message_date, job_flag, sleep_seconds=0):
        try:
            self.file_name = self.local_file_path + date_f(0)[0] + '_' + job_flag + '.csv'
            with open(self.file_name, 'w', newline='') as file_1:
                writer_csv = csv.writer(file_1)
                writer_csv.writerow([self.title])
                for row in message_date:    # tqdm(message_date, ncols=80):
                    writer_csv.writerow([row])  # csv提供的写入方法可以按行写入list，无需按照对象一个个写入，效率更高
                    sleep(sleep_seconds)

                # for row in tqdm(iterable=message_date, ncols=80):
                #     writer_csv.writerow(row)  # csv提供的写入方法可以按行写入list，无需按照对象一个个写入，效率更高
                #     # sleep(0.05)

                # file_size = len(message_date)
                # for row in range(file_size):
                #     writer_csv.writerow(message_date[row])
                #     sys.stdout.write('\r[{0}] Percent:{1}%'.format('='*int(row*50/(file_size-1)), str(row*100/(file_size-1))))
                #     if row == file_size:
                #         sys.stdout.write('\r[{0}] Percent:{1}%'.format('=' * int(100), str(100)))
                #         print('\n')
                #     sleep(sleep_seconds)

        except Exception as e:
            print(e)


class MailSender(object):
    def __init__(self, mail_attach, *file_name):  # 邮件概览/正文/文件名(含路径)
        # self.mail_view = mail_view
        # self.mail_text = mail_text
        # self.mail_title = mail_title
        self.mail_attach = mail_attach
        self.file_name = file_name

    def mail_mime_prepare(self, receivers):
        mail_sender = 'shinnosuke1028@qq.com'
        mail_password = 'ixwzutghdbtxbaie'
        # receivers = ['zhyabs@gmail.com','717648387@qq.com']
        # receivers = ['717648387@qq.com', 'yangqidong@inspur.com']    # '89304594@qq.com'
        # receivers = ['717648387@qq.com']
        mail_server = 'smtp.qq.com'
        # subject = 'Python SMTP 邮件测试...<数据完整性监控(日常JOB/采集)>'
        subject = 'Py-%s <数据完整性监控(日常JOB/昨日采集)>' % date_f(0)[0]

        # MIMEMultipart 形式构造邮件正文
        msg = MIMEMultipart()  # 开辟一个带邮件的mail接口
        msg['From'] = formataddr(['郭皓然测试', mail_sender])
        msg['To'] = formataddr([','.join(receivers), 'utf-8'])  # 用','进行拼接，待拼接内容：join(x)内的x
        msg['Subject'] = Header(subject, 'utf-8')
        # msg.attach(MIMEText(self.mail_view + '\n' + self.mail_title + self.mail_text, 'plain', 'utf-8'))
        msg.attach(MIMEText(self.mail_attach, 'plain', 'utf-8'))

        try:
            server = smtplib.SMTP(mail_server, 25)  # 发件人邮箱中的SMTP服务器，SMTP服务端口是25
            server.login(mail_sender, mail_password)  # 括号中对应的是发件人邮箱账号、邮箱密码
            server.sendmail(mail_sender, receivers, msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、邮件内容发送
            print('邮件发送成功.')
            server.quit()  # 关闭连接
        except Exception as e:
            print(e)
            print('Error:邮件发送失败...')


# 以下是装饰器修饰函数的用法，可省略代码的反复加工
def lock_f(thread='Y'):
    def threading_f(f):
        def inner_func(*value):
            if thread == 'Y':
                r_lock = threading.RLock()
                with r_lock:
                    r_lock.acquire()
                    # print('{} 函数实例化开始..'.format(f.__name__))
                    print('Thread', threading.current_thread().getName(), 'IS Running. Time: %s' % ctime())
                    # print('Thread ', threading.current_thread().name, ' with rlock..')
                    f(*value)
                    # print('{} 函数实例化结束..'.format(f.__name__))
                    r_lock.release()
                    print('Thread', threading.current_thread().getName(), 'End. Time: %s' % ctime())
            else:
                # print('{} 函数实例化开始..'.format(f.__name__))
                # print('Thread ', threading.current_thread().name, ' with rlock..')
                print('Thread', threading.current_thread().getName(), 'IS Running. Time: %s' % ctime())
                f(*value)
                # print('{} 函数实例化结束..'.format(f.__name__))
                print('Thread', threading.current_thread().getName(), 'End. Time: %s' % ctime())

        return inner_func

    return threading_f


@lock_f('Y')
def ora_job(conf_job, file_path, file_title):
    # 实例化
    ora = FileWR(local_file_path=file_path, title=file_title)
    ora.conf_f = conf_job
    job_flag = ora.conf_f[2]
    ora.connect_f()
    file_mail_view_tmp = str(ora.execute_f())
    del ora.message
    # print('1:', file_mail_view_tmp)
    file_mail_text_tmp = ora.execute_split_f()
    del ora.message_data
    ora.file_write_f(file_mail_text_tmp, job_flag,)
    return file_mail_view_tmp, file_mail_text_tmp


def progressbar(cur, total):
    percent = '{:.2%}'.format(cur / total)
    sys.stdout.write('\r')
    sys.stdout.write('[%-50s] %s' % ('=' * int(math.floor(cur * 50 / total)), percent))
    sys.stdout.flush()
    if cur == total:
        sys.stdout.write('\n')


# if __name__ == '__main__':
#     # print(sys.path)
#     print('Thread', threading.current_thread().getName(), 'IS Running. Time: %s' % ctime())
#
#     threads = []
#     t2 = None
#
#     # 实例化信息&检索语句初始化
#     sqlConf = {
#         'CONF_JOB': sql_conf.conf_JOB,
#         'CONF_PKG': sql_conf.conf_PKG,
#         'CONF_GATHER': sql_conf.conf_GATHER,
#         'CONF_SCHEDULER': sql_conf.conf_SCHEDULER
#     }
#     # 文件生成路径
#     filePath = bas_mail_conf.mail_file_path_class
#
#     # 各报表标题初始化
#     fileTitleJob = {
#         'CONF_JOB': bas_mail_conf.file_title_job,
#         'CONF_PKG': bas_mail_conf.file_title_pkg,
#         'CONF_GATHER': bas_mail_conf.file_title_gather,
#         'CONF_SCHEDULER': bas_mail_conf.file_title_scheduler
#     }
#
#     # 线程初始化
#     for rs in sqlConf.keys():
#         # print(rs)
#         # print('%s配置:' % rs, sqlConf[rs])
#         # print('%sTITLE配置:' % rs, fileTitleJob[rs])
#         t = threading.Thread(target=ora_job, args=(sqlConf[rs], filePath, fileTitleJob[rs]))
#         # print(t.getName(), '\n')
#         threads.append(t)
#
#     # 线程批量启动
#     for rt in threads:
#         rt.start()
#
#     for rt in threads:
#         rt.join()
#
#     ######################################################
#     # # 单线程
#     # for rs in sqlConf.keys():
#     #     ora_job(sqlConf[rs], filePath, fileTitleJob[rs])
#
#     print('Thread', threading.current_thread().getName(), 'End. Time: %s' % ctime())
#
#     # # 测试
#     # t = threading.Thread(target=ora_job, args=(sqlConf['CONF_PKG'], filePath, fileTitleJob['CONF_PKG']))
#     # t.start()
#     # t.join()
#
#     # print(threads)
#     # print(date_f()[1])
