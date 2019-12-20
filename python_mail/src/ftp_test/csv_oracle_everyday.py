# -*- coding: utf8 -*-
import cx_Oracle
import csv
from time import sleep
from tqdm import tqdm


# 编码转换
# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

# local_file_path = r'D:\FTP'
# file_name = local_file_path + '\\' + 'test.csv'
# csv_file = open('D:\\FTP\\test.csv', 'rt')

# 这里同样的，tqdm就是这个进度条最常用的一个方法
# 里面存一个可迭代对象


with open('D:\\FTP\\CSV_Oracle\\test.csv', 'rt') as csv_file:
    # 此处打开模式为文本模式，不可用二进制模式 rb 打开
    # csv_file = pd.read_csv('D:\\FTP\\test.csv')
    reader = csv.reader(csv_file)   # 按行索引
    M = []
    header_row = next(reader)       # 跳过标题行
    print('Title:' + str(header_row) + '\n')    # 跳过之后，下列循环内不再索引标题行
    i = 0
    for rs in reader:
        # print(rs)
        try:
            M.append((rs[0], rs[1], rs[2], rs[3]))
            if i <= 3:
                print('M[' + str(i) + ']:' + str(M[i]))
                print('row[0]:' + rs[0])
                print('row[1]:' + rs[1])
                print('row[2]:' + rs[2])
                print('row[3]:' + rs[3] + '\n')
                i = i + 1
            else:
                i = i + 1
        except AttributeError as A:
            pass
            print(A)
    print('数据行数：' + str(i))

    # list测试
    for rs2 in M:
        print(rs2)
    print('M测试：' + str(M[3:6]))   # [m:n]:['',''] list内的索引为m至n-1的元素
    # print(M[0:2])

    # j = 0
    # for rs in M:
    #     j = j + 1
    #     for j in range(0, 10):
    #         print(j)
    #         # print(rs)
    #         j = j + 1



    # # 创建数据库连接
    # ip = "192.168.62.53"
    # username = "LRNOP"
    # password = "Inspur*()890"
    # oracle_port = "1521"
    # oracle_service = "SHIRNOP"
    # db = cx_Oracle.connect(username + "/" + password + "@" + ip + ":" + oracle_port + "/" + oracle_service)
    #
    # # 获取操作游标
    # cursor = db.cursor()

# print len(M)
#
# print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
# print '===begin==='
#
# cursor.prepare("INSERT INTO MY_TABLE (ID, COMPANY, DEPARTMENT, NAME) VALUES (:1,:2,:3,:4)")
#
# print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
# print 'prepare end'
#
# for i in range(1, 31):
#     begin = (i - 1) * 30000
#     end = i * 30000
#     cursor.executemany(None, M[begin:end])
#     print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), '=>', begin, '-', end, '(',  len(M[begin:end]), ')','finish'
#
# print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
# print 'execute end'
#
# conn.commit()
# #885640
# print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
# print 'end'
#
# r = cursor.execute("SELECT COUNT(*) FROM MY_TABLE")
# print cursor.fetchone()
#
# #关闭连接，释放资源
# cursor.close()
# conn.close()