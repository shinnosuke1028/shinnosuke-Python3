# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 19/10/15 12:13
# @File: func_ppt2png.py

# import win32com
# import win32com.client
from win32com import client
import os

# 存储PPT为JPG格式的类型，所对应的数值
ppSaveAsJPG = 17
output_dir_name = r'D:\FTP\PPT2PNG\HR_TEST.pptx'

# def ppt2png(ppt_path, long_format: str):


def ppt2png(ppt_path):
    if os.path.exists(ppt_path):
        ppt_app = client.Dispatch('PowerPoint.Application')
        # 设置为0表示后台运行，不显示，1则显示
        ppt_app.Visible = 1
        # 打开PPT文件
        ppt = ppt_app.Presentations.Open(ppt_path)
        # 另存为，第一个参数为报存图片的目录，第二个是报存的格式。
        # ppt.SaveAs(output_dir_name, ppSaveAsJPG)
        # 退出PPT
        ppt_app.Quit()
        return ppt


if __name__ == '__main__':
    PPT = ppt2png(output_dir_name)
    print(PPT)
