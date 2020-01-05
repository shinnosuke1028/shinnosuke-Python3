# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/9/21 9:17
# @File: sql_conf.py
"""
数据查询内容配置
"""
# 数据库连接信息
connect_info = 'LRNOP/Inspur*()890@192.168.62.53:1521/SHIRNOP'

# 查询SQL
sql_pkg = '''select * from db_check t where t.EXECUTE_SDATE >= trunc(sysdate)'''

sql_job = '''select job, '4G' as owner, failures, next_date, interval, what from user_jobs j where BROKEN='N' union all
             select job, '3G' as owner, failures, next_date, interval, what from user_jobs@wrnop_44 k union all
             select job, '2G' as owner, failures, next_date, interval, what from user_jobs@grnop_38 l
          '''

sql_gather = '''select * from CSV_CHECKRESULT_HOUR'''

sql_export = '''select * from TB_EXPORT_VIEW where s_date = trunc(sysdate) and rownum <= 500'''

sql_log = '''select BLOB_TO_VARCHAR2(binary_output) from SYS.DBA_SCHEDULER_JOB_RUN_DETAILS db where owner = 'LRNOP' and to_char(db.log_date, 'yyyymmdd') = to_char(sysdate,'yyyymmdd')'''

sql_scheduler = ''' SELECT t.LOG_DATE, t.JOB_NAME, t.STATUS, t.RUN_DURATION, t.NEXT_RUN_DATE FROM SCHEDULER_LOG_VIEW t WHERE LOG_DATE >= TRUNC(SYSDATE)'''

conf_PKG = [connect_info, sql_pkg, 'PKG']
conf_JOB = [connect_info, sql_job, 'JOB']
conf_GATHER = [connect_info, sql_gather, 'GATHER']
conf_LOG = [connect_info, sql_log, 'LOG']
conf_SCHEDULER = [connect_info, sql_scheduler, 'SCHEDULER']

sqlDict = {
    'CONF_JOB': conf_JOB,
    # 'CONF_PKG': conf_PKG,
    # 'CONF_GATHER': conf_GATHER,
    # 'CONF_SCHEDULER': conf_SCHEDULER
}
