# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/5 18:07
# @File: Oracle2File.py
# @Usage:

import sys
import math
# from os.path import abspath, join, dirname
import os, re
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

sys.path.append('./func_demo')
from src.func_demo.func_f import date_f


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

    def file_write_f(self, message_date, job_flag, sleep_seconds=0.001):
        """

        :param message_date:    <class: list> IN: [[],[],...,[]]
        Ex:
            # Each list represents for one row:
            [['NSN_CM', '4G_output_20191225.xml.gz'], ['NSN_CM', '34G_output_20191225.xml.gz'],...]

        :param job_flag:        <class: str>
        :param sleep_seconds:   <class: int>

        :return:

        """
        try:
            self.file_name = self.local_file_path + date_f(0)[0] + '_' + job_flag + '.csv'
            # print(self.file_name)
            with open(self.file_name, 'w', newline='', encoding='GBK') as f:
                writer_csv = csv.writer(f)
                writer_csv.writerow(self.title)
                #     # print([row])
                writer_csv.writerows(message_date)  # 也可以一口气写入 IN: [[],[],...,[]]
            return 0

                # for row in tqdm(message_date, ncols=80):
                #     writer_csv.writerow(row)  # csv提供的写入方法可以按行写入list，无需按照对象一个个写入，效率更高
                #     sleep(sleep_seconds)
        except Exception as e:
            print('Status: FileWR.file_write_f!')
            print('------------------' * 2, f'\nError Details:\n{e}')
            print('------------------' * 2)
            return 1


# class MailSender(object):
#     def __init__(self):  # 邮件概览/正文/文件名(含路径)
#         # self.mail_view = mail_view
#         # self.mail_text = mail_text
#         # self.mail_title = mail_title
#         self.mail_attach = []
#         # self.file_name = file_name
#         self.msg = None
#         # self.attach = None
#
#     def mail_mime_action(self, receivers):
#         mail_sender = 'shinnosuke1028@qq.com'
#         mail_password = 'ixwzutghdbtxbaie'
#         mail_server = 'smtp.qq.com'
#         # subject = 'Python SMTP 邮件测试...<数据完整性监控(日常JOB/采集)>'
#         subject = 'Py-%s <数据完整性监控(日常JOB/昨日采集)>' % date_f(0)[0]
#
#         # MIMEMultipart 形式构造邮件正文
#         self.msg = MIMEMultipart()  # 开辟一个带邮件的mail接口
#         self.msg['From'] = formataddr(['郭皓然测试', mail_sender])
#         self.msg['To'] = formataddr([','.join(receivers), 'utf-8'])  # 用','进行拼接，待拼接内容：join(x)内的x
#         self.msg['Subject'] = Header(subject, 'utf-8')
#
#         # 邮件正文
#         # self.msg.attach(MIMEText(message + '\n' + title + message_str, 'plain', 'utf-8'))
#
#         # 邮件装载附件
#         # 1
#         # for fn in self.file_name:
#         #     attach_tmp = self.msg_attach(fn)
#         #     self.mail_attach.append(attach_tmp)
#
#         # 2
#         try:
#             cur_list_re = []
#             for fn in os.walk(bas_mail_conf.mail_file_path_class):
#                 print(f'fn[-1]: {fn[-1]}')  # ./data_output/*
#                 for cur in fn[-1]:
#                     x = re.search(bas_mail_conf.file_pattern, cur)
#                     print(f'cur: {cur}, x: {x}')
#                     # cur: 20191217_PKG.csv, x: None
#                     # cur: 20191218_GATHER.csv, x: <re.Match object; span=(0, 19), match='20191218_GATHER.csv'>
#                     if x:
#                         cur_list_re.append(bas_mail_conf.mail_file_path_class + x.group())
#                     else:
#                         continue
#             for rx in cur_list_re:
#                 # print(f'rx: {rx}')
#                 tx_tmp = self.msg_attach(rx)
#                 self.msg.attach(tx_tmp)
#             print(f'Status: Mail loaded successfully.')
#         except Exception as e:
#             print(f'Status: Failed to load mail...')
#             print('------------------' * 2, f'\nError Details:\n{e}')
#             print('------------------' * 2)
#
#         try:
#             server = smtplib.SMTP(mail_server, 25)  # 发件人邮箱中的SMTP服务器，SMTP服务端口是25
#             server.login(mail_sender, mail_password)  # 括号中对应的是发件人邮箱账号、邮箱密码
#             server.sendmail(mail_sender, receivers, self.msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、邮件内容发送
#             print('Status: Mail sended successfully.')
#             server.quit()  # 关闭连接
#         except Exception as e:
#             print('Status: Failed to send mail...')
#             print('------------------' * 2, f'\nError Details:\n{e}')
#             print('------------------' * 2)
#
#     def msg_attach(self, file_name):
#         """
#
#         :return:  type(attach): <class 'email.mime.text.MIMEText'> 附件封装结果
#
#         Ex:
#             att1 = MIMEText(open(file_name, 'rb').read(), 'base64', 'utf-8')
#             att1["Content-Type"] = 'application/octet-stream'
#             att1["Content-Disposition"] = 'attachment; filename=' + file_name    # 这里的filename可任意，写什么名字，邮件中显示什么名字
#             msg.attach(att1)
#
#             att2 = MIMEText(open(file_name2, 'rb').read(), 'base64', 'utf-8')
#             att2["Content-Type"] = 'application/octet-stream'
#             att2["Content-Disposition"] = 'attachment; filename=' + file_name2    # 这里的filename可任意，写什么名字，邮件中显示什么名字
#             msg.attach(att2)
#
#         """
#         attach = MIMEText(open(file_name, 'rb').read(), 'base64', 'utf-8')
#         attach["Content-Type"] = 'application/octet-stream'
#         attach["Content-Disposition"] = 'attachment; filename=' + file_name  # 这里的filename可任意，写什么名字，邮件中显示什么名字
#         # print(f'type(attach): {type(attach)}')
#         return attach


class MyThread(threading.Thread):
    def __init__(self, func=None, args=()):
        super().__init__()
        self.func = func
        self.args = args
        self.result = []

    def run(self):
        try:
            print(f'Status: 线程入口.')
            self.result = self.func(*self.args)
        except Exception as e:
            # raise (f'Exception: {str(e)}')
            raise ('Bad execution: %s' % str(e))

    def get_result(self):
        # noinspection PyBroadException
        try:
            print(f'Results return.')
            return self.result
        except Exception as e:
            print(f'Status: 线程返回结果.')
            print('------------------' * 2, f'\nError Details:\n{e}')
            print('------------------' * 2)
            return 1


######################################################
######################################################


# 以下是装饰器修饰函数的用法，可省略代码的反复加工
balance = []
i = 0


def lock_f(lock_flag='N'):
    def threading_f(f):
        def inner_f(*value):
            print('Status: 1号装饰器测试开始！')
            global balance, i
            if lock_flag == 'Y':
                i += 1
                lock = threading.RLock()
                with lock:
                    # r_lock.acquire()
                    print(f'Thread {threading.current_thread().getName()} is running. Time: {ctime()}')
                    result = f(*value)
                    # pprint.pprint(results)
                    # print(type(results))    # <class 'tuple'>
                    balance.append(result)
                    # r_lock.release()
                    print(f'Thread {threading.current_thread().getName()} end. Time: {ctime()}')
                    print('1号装饰器测试结束！')
            else:
                print('Status: 1号装饰器不再调用！')
                print(f'Thread {threading.current_thread().getName()} is running. Time: {ctime()}')
                result = f(*value)
                balance.append(result)
                print(f'Thread {threading.current_thread().getName()} end. Time: {ctime()}')
            # pprint.pprint(result)
            return balance

        return inner_f

    return threading_f


# def email_f(email_flag='N'):
#     def mail_post_f(f):
#         def inner_f(*value):
#             print('Status: 2号装饰器测试开始！')
#             if email_flag == 'Y':
#                 print(f'Thread {threading.current_thread().getName()} is running. Time: {ctime()}')
#                 results = f(*value)
#                 # mail = None
#                 # 注意: 这里要捕捉的是文件接口,不是数据详情,只需提供返回的文件名即可
#                 # 部分邮件正文才需要从上述results中获取,这里先不添加正文信息
#                 mail = MailSender()
#                 mail.mail_mime_action(bas_mail_conf.receivers)
#
#                 print(f'Thread {threading.current_thread().getName()} end. Time: {ctime()}')
#                 print('2号装饰器测试结束！')
#
#             else:
#                 print('Status: 2号装饰器不再调用！')
#                 print(f'Thread {threading.current_thread().getName()} is running. Time: {ctime()}')
#                 results = f(*value)
#                 print(f'Thread {threading.current_thread().getName()} end. Time: {ctime()}')
#             # pprint.pprint(results)
#             return results
#
#         return inner_f
#
#     return mail_post_f


# 程序编译时优先编译内层装饰器/再编译外层装饰器,编译顺序:email_f -> lock_f
# 执行时,类似于Queue(先进后出),执行顺序:lock_f(执行 f(*value) 之前的内容) -> email_f -> lock_f(f(*value))
# @email_f('Y')  # 1号装饰器
@lock_f('Y')  # 2号装饰器
def ora_job(conf_job, file_path, file_title):
    # with r_lock:
    # 实例化
    ora = FileWR(local_file_path=file_path, title=file_title)
    ora.conf_f = conf_job  # 列表按顺序，进行数据库检索配置
    job_flag = ora.conf_f[2]
    ora.connect_f()
    file_mail_view_tmp = str(ora.execute_f())
    del ora.message
    # print('1:', file_mail_view_tmp)
    file_mail_text_tmp = ora.execute_split_f()
    del ora.message_data
    # print(type(file_mail_text_tmp))
    ora.file_write_f(file_mail_text_tmp, job_flag, )
    return job_flag, file_mail_view_tmp, file_mail_text_tmp




def progressbar(cur, total):
    percent = '{:.2%}'.format(cur / total)
    sys.stdout.write('\r')
    sys.stdout.write('[%-50s] %s' % ('=' * int(math.floor(cur * 50 / total)), percent))
    sys.stdout.flush()
    if cur == total:
        sys.stdout.write('\n')


# if __name__ == '__main__':
#     # print(sys.path)
#     print('Thread', threading.current_thread().getName(), 'is Running. Time: %s' % date_f()[2])
#
#     ######################################################
#     # 准备工作
#     threads = []
#     # 实例化信息&检索语句初始化
#     sqlConf = sql_conf.sqlDict
#     # 文件生成路径/各报表标题初始化
#     filePath, fileTitleJob = bas_mail_conf.mail_file_path_class, bas_mail_conf.fileDict
#     ######################################################
#
#     ######################################################
#     # 采集多线程初始化
#     # r_lock = threading.RLock()
#     for rs in sqlConf.keys():
#         t = MyThread(ora_job, (sqlConf[rs], filePath, fileTitleJob[rs]))
#         threads.append(t)
#
#     # 线程批量启动
#     for rt in threads:
#         rt.start()
#
#     for rt in threads:
#         rt.join()
#
#     # mail_dict = {}
#     mail_dict_combine = []
#     # print(threads)
#     # for rt in threads:
#     #     # mail_dict['mail_attach_flag'] = rt.get_result()[0]
#     #     # mail_dict['mail_attach_view'] = rt.get_result()[1]
#     #     # mail_dict['mail_attach_text'] = rt.get_result()[2]
#     #
#     #     # 也可以使用临时字典新建内存,并保留当前字典键值对,再放入列表
#     #     # mail_dict_copy = copy.copy(mail_dict)
#     #     # mail_dict_combine.append(mail_dict_copy)
#     #
#     #     mail_dict_combine.append(
#     #         {
#     #             'mail_attach_flag': rt.get_result()[0],
#     #             'mail_attach_view': rt.get_result()[1],
#     #             'mail_attach_text': rt.get_result()[2]
#     #         }
#     #     )
#
#     # pprint.pprint(mail_dict_combine)
#     print('Thread', threading.current_thread().getName(), 'End. Time: %s' % date_f()[2])
#
#     # print(id(mail_dict_copy))    # 这里打印的内存值不同
#
#     # # 下次遍历前初始化字典的写法
#     # mail_dict_combine.append(mail_dict)
#     # mail_dict = {}
#     # print(id(mail_dict_combine))    # 这里打印的内存值虽相同,但??????
#
#     # # 注意:下面的写法有问题
#     # 若直接mail_dict_combine.append(mail_dict),会出现覆盖情况,数据始终指向mail_dict初始内存,故只能取到最后一组数据
#     # mail_dict_combine.append(mail_dict)
#     # print(id(mail_dict_combine)) # 即多次打印这里的内存值相同
#
#     # print(mail_dict_combine)
#
#     ######################################################
#     ######################################################
#     # # 单线程
#     # for rs in sqlConf.keys():
#     #     ora_job(sqlConf[rs], filePath, fileTitleJob[rs])
#     ######################################################
