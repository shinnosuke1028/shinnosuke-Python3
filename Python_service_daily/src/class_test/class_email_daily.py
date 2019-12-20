# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/9/5 8:08
# @File: class_email_daily.py

import cx_Oracle
import csv
import smtplib
# import numpy as np
import threading

from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart

from time import sleep
from tqdm import tqdm

# 配置
from src.func_test.func_f import date_f
from src.conf import bas_mail_conf
from src.conf import sql_conf
from src.conf import bas_insert_conf


class OracleExecution(object):
    def __init__(self, connect=None, sql=None, check_style=None):
        self.sql = sql
        self.connect = connect
        # self.title = title
        self.check_style = check_style
        # self.message_str = ''
        self.rs = []
        self.message = ''
        self.message_data = []
        self.db = None

    def connect_f(self):
        try:
            self.db = cx_Oracle.connect(self.connect)
        except Exception as e:
            print(e)
        finally:
            return self.db

    def execute_f(self):
        # db = cx_Oracle.connect(self.username + "/" + self.password + "@" +
        try:
            cur = self.db.cursor().execute(self.sql)
        except Exception as e:
            print(e)
            self.db.rollback()
        else:
            self.rs = cur.fetchall()
            # print(self.rs)    # [] list类型
            if self.check_style == 'JOB':
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

    def file_write_f(self, message_date, sleep_seconds=0.04):
        try:
            self.file_name = self.local_file_path + '\\' + date_f(0)[0] + '_' + self.check_style + '.csv'
            with open(self.file_name, 'w', newline='') as file_1:
                writer_csv = csv.writer(file_1)
                writer_csv.writerow(self.title)
                for row in tqdm(message_date, ncols=80):
                    writer_csv.writerow(row)        # csv提供的写入方法可以按行写入list，无需按照对象一个个写入，效率更高
                    sleep(sleep_seconds)
        except Exception as e:
            print(e)
        else:
            file_1.close()


class MailSender(object):
    def __init__(self, mail_attach, *file_name):    # 邮件概览/正文/文件名(含路径)
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
        msg = MIMEMultipart()                                          # 开辟一个带邮件的mail接口
        msg['From'] = formataddr(['郭皓然测试', mail_sender])
        msg['To'] = formataddr([','.join(receivers), 'utf-8'])           # 用','进行拼接，待拼接内容：join(x)内的x
        msg['Subject'] = Header(subject, 'utf-8')
        # msg.attach(MIMEText(self.mail_view + '\n' + self.mail_title + self.mail_text, 'plain', 'utf-8'))
        msg.attach(MIMEText(self.mail_attach, 'plain', 'utf-8'))

        try:
            server = smtplib.SMTP(mail_server, 25)                              # 发件人邮箱中的SMTP服务器，SMTP服务端口是25
            server.login(mail_sender, mail_password)                              # 括号中对应的是发件人邮箱账号、邮箱密码
            server.sendmail(mail_sender, receivers, msg.as_string())       # 括号中对应的是发件人邮箱账号、收件人邮箱账号、邮件内容发送
            print('邮件发送成功.')
            server.quit()   # 关闭连接
        except Exception as e:
            print(e)
            print('Error:邮件发送失败...')


if __name__ == '__main__':
    try:
        # 实例化数据库检索初始化
        connect_info = sql_conf.connect_info

        # 库内execute内容初始化
        sql1 = sql_conf.sql_pkg
        sql2 = sql_conf.sql_job
        sql3 = sql_conf.sql_gather
        # sql4 = sql_conf.sql_export

        # 本地文件路径初始化
        # D:\\IdeaProjects\\Python_service_daily\\src\\class_test\\data
        file_path = bas_mail_conf.mail_file_path_class

        # 各报表标题初始化
        file_title_pkg = bas_mail_conf.file_title_pkg
        file_title_job = bas_mail_conf.file_title_job
        file_title_ga = bas_mail_conf.file_title_gather

        # 邮件正文初始化
        file_mail_text = ''

        # 写入附件文件准备工作
        # file_wr = FileWR(local_file_path=file_path, title
        # =file_title_pkg, connect=connect_info,check_style='PKG',sql=sql1)
        file_wr = FileWR(local_file_path=file_path, title=file_title_pkg)
        file_wr2 = FileWR(local_file_path=file_path, title=file_title_job)
        file_wr3 = FileWR(local_file_path=file_path, title=file_title_ga)
        # file_wr4 = FileWR(local_file_path=file_path, title='TB_LTE')

        file_wr.connect = file_wr2.connect = file_wr3.connect = connect_info
        # file_wr4.connect = connect_info

        # 执行 获取附件内容
        # 部分脚本执行监控

        file_wr.check_style = 'PKG'
        file_wr.sql = sql1
        file_wr.connect_f()
        file_wr.execute_f()
        del file_wr.message
        file_wr.execute_split_f()
        file_wr.file_write_f(file_wr.message_data)
        del file_wr.message_data    # 内存释放

        # JOB
        file_wr2.check_style = 'JOB'
        file_wr2.sql = sql2
        file_wr2.connect_f()
        file_mail_view_temp = str(file_wr2.execute_f()) # 邮件正文需要转换为字符串，以备后用
        # del file_wr2.message
        file_mail_text_temp = file_wr2.execute_split_f()
        del file_wr2.message_data    # 内存释放
        file_wr2.file_write_f(file_mail_text_temp)

        # Gather
        file_wr3.check_style = 'GA'
        file_wr3.sql = sql3
        file_wr3.connect_f()
        file_wr3.execute_f()
        del file_wr3.message
        file_wr3.execute_split_f()
        file_wr3.file_write_f(file_wr3.message_data)
        del file_wr3.message_data    # 内存释放

        # 选取第1个查询结果的 message 作为邮件概览，选取第2个查询结果的 message_data 作为邮件正文
        # 邮件正文字符串换行拼接
        for rs in file_mail_text_temp:
            file_mail_text = file_mail_text + str(rs) + '\n'
        # print(file_mail_text)

        # 邮件概览/标题/正文字符串拼接
        mail_attach_message = file_mail_view_temp + '\n' + str(file_title_job) + '\n' + file_mail_text
        # print('邮件正文拼接结果：', '\n', mail_attach_message)
        print('<' + date_f(0)[1] + '> : ' + '%s任务完成.' % date_f(0)[0])

        # file_wr4.check_style = 'TB_LTE'
        # file_wr4.sql = sql4
        # file_wr4.connect_f()
        # file_wr4.execute_f()
        # del file_wr4.message
        # file_wr4.execute_split_f()
        # file_wr4.file_write_f(file_wr4.message_data)
        # del file_wr4.message_data    # 内存释放

        # file_wr4 = FileWR(local_file_path=file_path, title=file_title_pkg)
        # file_wr4.check_style = 'TEST'
        # file_wr4.file_write_f([('2019/9/15 7:00', 'TEST', '1', '6028')])
        # print('<' + date_f(0)[1] + '> : ' + 'Writing Complete!!!')

        mail_job = MailSender()

    except Exception as e:
        print('<' + date_f(0)[1] + '> : ' + '%s任务完成.' % date_f(0)[0])
        print(e)
