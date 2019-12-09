# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/12/9 18:43
# @File: main_re.py
# @Usage: RE正则

import pandas as pd
from pandas import DataFrame


data_input_path = './data_output/20191209_FileList.csv'

with open(data_input_path, newline='', encoding="utf-8") as MyFile:
# data_df = pd.read_csv(data_input_path)
    line = MyFile.read()
    print(line)


