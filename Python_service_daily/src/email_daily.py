# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/8/15 12:40
# @File: email_everyday.py

import cx_Oracle
import smtplib                          # SMTP(Simple Mail Transfer Protocol) 负责发送邮件
import os

from email.mime.text import MIMEText    # 负责构造邮件
from email.header import Header
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart
from time import sleep
from tqdm import tqdm
# from collections import Counter

from func_test.func_f import date_f
from conf import bas_mail_conf

# 主文件引入其它.py内的module时，需区分文件层级，主文件需在最外层，再引入其它文件，不然cmd运行会报：ModuleNotFoundError: No module named 'xxx'
# Ex：主文件在src/mail/，待引用文件在src/func_test/，主文件内写成：from src.func_test.func_f import date_f 时，会报上述错误

# from email.mime.application import MIMEApplication
# import datetime
# import numpy as np

title = '执行时间, 执行包, 执行次数, 最终数据结果' + '\n'    # 注意，字符串元素内逗号要用英文类型，不可使用正文逗号，不然无法按照逗号分割，填入csv
title2 = 'S_DATE, data_style<file_path>, HOUR, 包内数据时间, NORMAL_FILE_NUM, NOW_NUM, BEF_NUM, NORMAL_FILE_SIZE/MB, ' \
         'NOW_SIZE/MB, BEF_SIZE/MB, FILE_NUM<今-昨>, ' \
         'FILE_SIZE<今-昨>/MB, FILE_NUM_STATUS, FILE_SIZE_STATUS, PATHNAME' + '\n'
title3 = 'JOB_ID, FAILURES, NEXT_DATE, INTERVAL, WHAT' + '\n'

local_file_path = r'D:\FTP\Mail'


def message_database_f(sql, check_style):
    # message_data若在函数体内需要被修改的话，需在函数体内global声明
    # 如果一个全局变量在函数内被重新定义过，再在函数内使用变量则默认为局部变量；
    # 如果在函数内没有被定义，直接使用会被视为全局变量。如果需要在函数内定义（修改）全局变量，则要先用global进行声明
    global message_data
    # global message_str
    # global file_name

    ip = "192.168.62.53"
    username = "LRNOP"
    password = "Inspur*()890"
    oracle_port = "1521"
    oracle_service = "SHIRNOP"

    # db=cx_Oracle.connect('LRNOP/Inspur*()890@192.168.62.53:1521/SHIRNOP')
    # 通过连接符拼接连接语句
    db = cx_Oracle.connect(username + "/" + password + "@" + ip + ":" + oracle_port + "/" + oracle_service)
    sql_string = sql
    try:
        cur = db.cursor().execute(sql_string)    # 创建cursor并执行
    except Exception as e:
        print(str(e))
        db.rollback()
    else:
        # cur.execute(sql_1)                # 执行sql语句
        rs = cur.fetchall()		            # rs:list类型，fetchall(self):接收全部的返回结果行：[(A),(B),...,(Z)]
        # print(str(rs))

        # 任务正确性简单判断
        message = ''
        if check_style == 'JOB':
            data_flag = 0
            i = 0
            for r1 in rs:
                if i == 0:
                    data_flag = r1[1]
                elif r1[1] is not None:
                    data_flag = data_flag + r1[1]
                else:
                    data_flag = data_flag + 0
                i = i + 1   # 数据行数，移动至下一行
            # print(data_flag)
            if data_flag != 0:
                message = '<' + date_f(0)[1] + '> : ' + '数据库脚本或定时任务异常,请及时核查.'
            else:
                message = '<' + date_f(0)[1] + '> : ' + '数据库脚本正常执行,详细监测日志请查看附件.'
            print(message)

        # else:
        #     message = '采集清单核查.'

        # log表详细内容获取
        message_data_all = []
        for r1 in rs:
            j = 0
            for r2 in r1:
                if j == 0:
                    # message_data = str(r2)
                    # list内包含多个元组，每个元组内含有对应的元素，最好在每个元素外层都加上" "，不然在写.csv时容易将元素对象所含有的','当成分隔符
                    # 不能用''分隔每一个，在.csv内依旧无法区分每一个元组内的每一个元素 <Excel 分隔符特性>
                    # Ex: [(21,0,TRUNC(sysdate,'hh'))] 中的第三个元素，其','号会在入.csv时作为分隔符，将该元素分割成两部分
                    message_data = '\"' + str(r2) + '\"'
                    # print(message_data)
                else:
                    # message_data = message_data + ',' + str(r2)
                    message_data = message_data + ',' + '\"' + str(r2) + '\"'
                    # print(message_data)
                j = j + 1
            message_data_all.append(message_data)          # 将结果保存于list内
            # print(message_data_all)
        message_data_all = '\n'.join(message_data_all)
        # print(message_data_all)

    # print(message_str)
        # 数组转字符串：
        # 结果按换行符分割，每一个内部元素内都还有','分隔，作为一个值填入csv的一个单元格
        # 若不分割，结果集将带有list符号：[]

        # print(message_data_all)                                 # ['A,'B','C']
        # print(message_str)                                      # A\n + B\n + C\n

        # 字符串转数组：
        # str = '1,2,3'
        # arr = str.split(',')
        # print a

        # 查询结果写入文件：201908xx.csv

        try:
            file_name = local_file_path + '\\' + date_f(0)[0] + '_' + check_style + '.csv'
            # print(file_name)
        except Exception as e:
            print(e)
        else:
            with open(file_name, 'w') as file_1:
                if check_style == 'JOB':
                    file_1.write(title3)
                else:
                    file_1.write(title2)
                for r3 in tqdm(message_data_all, ncols=80):
                    file_1.write(r3)
                    sleep(0.00000000000000000001)
            file_1.close()
            return file_name, message, message_data_all
        cur.close()
        db.close()


def mail_mime_f(receivers, message, message_str, file_name='', file_name2=''):
    mail_sender = 'shinnosuke1028@foxmail.com'
    mail_password = 'ixwzutghdbtxbaie'
    mail_server = 'smtp.qq.com'
    # subject = 'Python SMTP 邮件测试...<数据完整性监控(日常JOB/采集)>'
    subject = 'Py-%s <数据完整性监控(日常JOB/昨日采集)>' % date_f(0)[0]

    # MIMEMultipart 形式构造邮件正文
    msg = MIMEMultipart()                                          # 开辟一个带邮件的mail接口
    msg['From'] = formataddr(['郭皓然测试', mail_sender])
    msg['To'] = formataddr([','.join(receivers), 'utf-8'])           # 用','进行拼接，待拼接内容：join(x)内的x
    msg['Subject'] = Header(subject, 'utf-8')
    msg.attach(MIMEText(message + '\n' + title3 + message_str, 'plain', 'utf-8'))
    # file_name = local_file_path + date_f(0) + '.csv'

    # file_name_judge = [int(x) for x in (file_name, file_name2)]
    # print(file_name_judge)
    # print(message + '\n' + title3 + message_str)

    try:
        att1 = MIMEText(open(file_name, 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        att1["Content-Disposition"] = 'attachment; filename=' + file_name    # 这里的filename可任意，写什么名字，邮件中显示什么名字
        msg.attach(att1)

        att2 = MIMEText(open(file_name2, 'rb').read(), 'base64', 'utf-8')
        att2["Content-Type"] = 'application/octet-stream'
        att2["Content-Disposition"] = 'attachment; filename=' + file_name2    # 这里的filename可任意，写什么名字，邮件中显示什么名字
        msg.attach(att2)    # msg <class 'email.mime.multipart.MIMEMultipart'>
        # print(type(att2))   # att2 <class 'email.mime.text.MIMEText'>

    except Exception as e:
        print('att:' + str(e))

    try:
        server = smtplib.SMTP(mail_server, 25)                              # 发件人邮箱中的SMTP服务器，SMTP服务端口是25
        server.login(mail_sender, mail_password)                              # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(mail_sender, receivers, msg.as_string())       # 括号中对应的是发件人邮箱账号、收件人邮箱账号、邮件内容发送
        print('邮件发送成功.')
        server.quit()   # 关闭连接
    except Exception as e:
        print(e)
        print('Error:邮件发送失败...')


def file_rm_f():
    for path in os.walk(local_file_path):
        # print(path)
        for file_cur in path[-1]:
            # print(file_cur)   # ['20190905_Gather.csv', '20190905_PKG.csv', '20190906_PKG.csv']
            if file_cur.split('_')[0] != date_f(0)[0]:
                os.remove(path[0] + '\\' + file_cur)
            else:
                pass


def main_job_f():
    # sql_1 = 'select * from db_check t where t.EXECUTE_SDATE >= trunc(sysdate)'
    sql_2 = '''select * from CSV_CHECKRESULT_HOUR t'''
    sql_3 = '''select job, failures, next_date, '4G'owner, j.interval, what from user_jobs j where BROKEN='N' union all ''' \
            '''select job, failures, next_date, '3G'owner, k.interval, what from user_jobs@wrnop_44 k union all ''' \
            '''select job, failures, next_date, '2G'owner, l.interval, what from user_jobs@grnop_38 l '''
    # file_name, messages, message_data_all = message_database_f(sql_1, 'PKG')    # 返回数据库生成文件名(含路径)，告警正文，部分监控结果
    file_name3, messages, message_data_all = message_database_f(sql_3, 'JOB')
    file_name2 = message_database_f(sql_2, 'GATHER')[0]                         # 返回数据库生成文件名(含路径)，告警正文，部分监控结果

    mail_mime_f(bas_mail_conf.receivers, messages, str(message_data_all), file_name3, file_name2)
    print('任务完成时间：' + date_f(0)[1])
    file_rm_f()


if __name__ == '__main__':
    # path = sys.path
    # for rs in path:
    #     print(rs)
    # date_str = date_f()
    print('Customized Modules：' + date_f(0)[2])
    main_job_f()


# def mail_f(message, message_str):
#     mail_sender = '717648387@qq.com'
#     mail_password = 'ixwzutghdbtxbaie'
#     receivers = ['717648387@qq.com', 'yangqidong@inspur.com'] # yangqidong@inspur.com
#     # receivers = ['89304594@qq.com','717648387@qq.com','zhyabs@gmail.com']
#     mail_server = 'smtp.qq.com'
#     subject = 'Python SMTP 邮件测试...<数据完整性监控>'
#     # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
#     # msg = MIMEText('Python 邮件发送测试...\n'+ message_str, 'plain', 'utf-8')
#     msg = MIMEText(message + '\n' + title + '\n' + message_str, 'plain', 'utf-8')
#     msg['From'] = formataddr(['数据完整性监控', mail_sender])                  # 发送者
#     msg['To'] = formataddr(['Receivers:', ','.join(receivers)])             # 接收者
#     msg['Subject'] = Header(subject, 'utf-8')
#     try:
#         server = smtplib.SMTP(mail_server, 25)                              # 发件人邮箱中的SMTP服务器，SMTP服务端口是25
#         server.login(mail_sender, mail_password)                              # 括号中对应的是发件人邮箱账号、邮箱密码
#         server.sendmail(mail_sender, receivers, msg.as_string())       # 括号中对应的是发件人邮箱账号、收件人邮箱账号、邮件内容发送
#         print('邮件发送成功.')
#         server.quit()   # 关闭连接
#     except Exception as e:
#         print(e)
#         print('Error:邮件发送失败...')
