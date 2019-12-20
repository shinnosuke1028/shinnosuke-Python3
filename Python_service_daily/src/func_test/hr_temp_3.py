# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/12/2 15:46
# @File: hr_temp_3.py

# 字典排序
a = {'1': 'STYLE',
     '2': ('NSN_4G_PM', 'HW_4G_PM<OMC1>', 'HW_4G_PM<OMC2>', 'NSN_4G_CM', 'HW_4G_CM<OMC1>', 'HW_4G_CM<OMC2>'),
     '3': ('RED', 'ORANGE', 'SKYBLUE', 'GREEN', 'CHOCOLATE', 'BLUEVIOLET')
     }

# for k, v in a.items():
k = sorted(a.keys())[0]
print(a[k])
print(len(a))
