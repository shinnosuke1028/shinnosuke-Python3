# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/11/26 17:05
# @File: mysql_2_oracle.py

import cx_Oracle
import MySQLdb
from src.class_email_daily_3 import OracleExecution

# ora = OracleExecution()

if __name__ == '__main__':
    db1 = MySQLdb.connect(host='192.168.62.74', user='root', passwd='Inspur*()890', db='test', port=5029)
    cursor1 = db1.cursor()
    # 定义查询语句
    cursor1.execute('select * from test.income_test')
    mysql_data = cursor1.fetchall()
    db1.close()
    for rs in mysql_data:
        print(rs)

