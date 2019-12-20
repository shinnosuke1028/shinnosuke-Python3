# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/9/8 17:04
# @File: threading_c.py

import datetime
import time
import threading

lock = threading.Lock()


def loop():
    with lock:
        begin = datetime.datetime.now()
        begin_count = time.time()
        print('Start: ', begin.strftime('%Y%m%d %H:%M:%S:%f'))
        print('Thread', threading.current_thread().getName(), 'is running.')
        n = 0
        while n < 1000000000:
            # print('Thread',threading.current_thread().getName(), ':%d' % n)
            n += 1

        print('Thread', threading.current_thread().name, ' :%d' % n)
        end_count = time.time()
        end = datetime.datetime.now()
        print('End: ', end.strftime('%Y%m%d %H:%M:%S:%f'))
        print('Loop total time: %.2f'% (end_count - begin_count))
        print('Thread', threading.current_thread().getName(), 'End.', '\n')


# class Producer():
print('Thread', threading.current_thread().getName(), 'is running.', '\n')

t = threading.Thread(target=loop, name='Loop_thread1')
t2 = threading.Thread(target=loop, name='Loop_thread2')
t.start()
t2.start()

t.join(3)
t2.join(3)

print('Thread', threading.current_thread().getName(), 'End.')
