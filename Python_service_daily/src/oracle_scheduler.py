# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/11/7 9:36
# @File: oracle_scheduler.py

import threading


from src.class_test.class_email_daily_2 import FileWR
# 配置
from src.conf import bas_mail_conf
from src.conf import sql_conf


# 以下是装饰器修饰函数的用法，可省略代码的反复加工
def threading_f(f):
    lock = threading.RLock()

    def inner_func(*value):
        with lock:
            lock.acquire()
            print('{} 函数实例化开始..'.format(f.__name__))
            # print('Thread ', threading.current_thread().name, ' with rlock..')
            f(*value)
            lock.release()
            print('{} 函数实例化结束..'.format(f.__name__))
    return inner_func


def fun_var(lang='C'):
    def main_job_f(f):
        def inner_func(*value):
            if lang == 'C':
                print('{} 函数实例化开始..'.format(f.__name__.upper()))
            else:
                print('{} Function Initialized..'.format(f.__name__.upper()))
            # print('Thread ', threading.current_thread().name, ' with rlock..')
            f(*value)
            if lang == 'C':
                print('{} 函数实例化结束..'.format(f.__name__.upper()))
            else:
                print('{} Function Initialization Finished..'.format(f.__name__))
        return inner_func
    return main_job_f


# @threading_f
@fun_var('E')
def ora_job(conf_job, file_path, file_title, lock):
    # 实例化(重入锁)
    with lock:
        lock.acquire()
        ora = FileWR(local_file_path=file_path, title=file_title)
        ora.conf_f = conf_job
        job_flag = ora.conf_f[2]
        # print(job_flag)
        ora.connect_f()
        file_mail_view_tmp = str(ora.execute_f())
        # print(file_mail_view_tmp)
        del ora.message
        # print('1:', file_mail_view_tmp)
        file_mail_text_tmp = ora.execute_split_f()
        del ora.message_data
        ora.file_write_f(file_mail_text_tmp, job_flag)
        lock.release()
    return file_mail_view_tmp, file_mail_text_tmp


if __name__ == '__main__':
    # 实例化信息&检索语句初始化
    confJob = sql_conf.conf_SCHEDULER

    # 文件生成路径
    filePath = bas_mail_conf.mail_file_path_class

    # 各报表标题初始化
    fileTitleJob = bas_mail_conf.file_title_scheduler

    r_lock = threading.RLock()

    # 后续在这里以循环的形式建立线程，并传入重入锁和各自的参数
    # for i in range(5):
    #     # 把创建的锁对象当做参数传递给函数
    #     t = threading.Thread(target=func, args=(lock,))
    #     t.start()

    t1 = threading.Thread(target=ora_job, args=(confJob, filePath, fileTitleJob, r_lock,))
    t1.start()
    t1.join()
