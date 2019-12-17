# -*- coding: utf-8 -*-
import cx_Oracle

import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

import schedule
import datetime

# message_tb = '日期,road_name,luduan_name,语音识别出的用户数,总通话次数,
# 掉话次数,掉话比,短通8秒,8秒短通比,两两短通总通话次数,两两短通8秒次数,两两短通8秒比例,MR3G_记录数,RSCP_DBM平均值,ECIO平均值\n'
base_file_path = r'E:\gongzuo\py_job_every_day\attached_files'
message_text_body = ''


def message_tbbase_f():
    global message_tb
    # message_tb若在函数体内需要被修改的话，需在函数体内global声明
    # 如果一个全局变量在函数内被重新定义过，再在函数内使用变量则默认为局部变量；
    # 如果在函数内没有被定义，直接使用会被视为全局变量。如果需要在函数内定义（修改）全局变量，则要先用global进行声明

    global message_text_body
    # bigmessage_tb_tns=cx_Oracle.makedsn('192.168.62.53',1521,'SHIRNOP')
    # db=cx_Oracle.connect('LRNOP','Inspur*()890',bigmessage_tb_tns)
    db=cx_Oracle.connect('LRNOP/Inspur*()890@192.168.62.53:1521/SHIRNOP')
    cr = db.cursor()        #创建cursor
    sql_1 = 'select job, failures, last_date, next_date, total_time t_time, j.interval, what from user_jobs j'

    cr.execute(sql_1)       # 执行sql语句
    rs = cr.fetchall()      # rs:list类型
    print(rs)
    if len(rs)>0:
        message_text_body='数据库脚本正常执行,详细数据请查看附件'
    else:
        message_text_body='数据库脚本有误,需及时查看'
    print(message_text_body)

    for r1 in rs:           # r1:元祖
        i = 0
        for r2 in r1:       # r2:元素
            if i == 0:
                # global message_tb
                message_tb = message_tb + str(r2)
                print(message_tb)
            else:
                message_tb = message_tb + ',' + str(r2)
                print(message_tb)
            i = i + 1
        message_tb = message_tb + '\n'      # 最后添加换行符
    file_1_path = base_file_path + '\\' + datetime.datetime.now().strftime('%Y%m%d') + '.csv'
    file_1 = open(file_1_path, 'w')
    file_1.write(message_tb)
    file_1.close()

    cr.close()
    db.close()


def email_f(message_text):
    sender = 'liangren_lr@163.com'
    passwd = 'liangren123'			                # 授权码是用于登录第三方邮件客户端的专用密码。
    mailserver = 'smtp.163.com'
    port = '25'
    receives = ["1610982938@qq.com","liangren_lr@163.com","panh8@chinaunicom.cn"]
    sub = 'LR Python3 test'
    msg = MIMEMultipart('related')
    msg['From'] = formataddr(["sender", sender])

    msg['To'] = ','.join(receives)
    msg['Subject'] = sub
    txt = MIMEText(message_text, 'plain', 'utf-8')  # txt:email.mime.text.MIMEText 类型
    msg.attach(txt)

    # 添加附件地址1
    part1 = MIMEApplication(open(base_file_path + '\\' + datetime.datetime.now().strftime('%Y%m%d') + '.csv', 'rb').read())
    part1.add_header('Content-Disposition', 'attachment', filename=datetime.datetime.now().strftime('%Y%m%d') + '.csv') # 发送文件名称
    msg.attach(part1)

    # 添加附件地址2
    # part2 = MIMEApplication(open(r'E:\gongzuo\py_job_every_day\attached_files\xx_2.csv', 'rb').read())
    # part2.add_header('Content-Disposition', 'attachment', filename="xx_2.csv")  # 发送文件名称
    # msg.attach(part2)

    server = smtplib.SMTP(mailserver, port)
    server.login(sender, passwd)
    server.sendmail(sender, msg['To'].split(','), msg.as_string())

    server.quit()
    print(datetime.datetime.now().strftime('%Y%m%d %H:%M:%S') + 'success')


def main_job_f():
    message_tbbase_f()
    # email_f(message_text_body)
    date_f()


def date_f():
    print(datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'))


if __name__ == '__main__':
    schedule.every(1).hour.do(date_f)
    schedule.every().day.at('11:50').do(main_job_f)
    while True:
        schedule.run_pending()
