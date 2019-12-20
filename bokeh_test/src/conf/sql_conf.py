# -*- coding:utf-8 -*-
# @Author: Shin
# @Date: 2019/9/21 9:17
# @File: sql_conf.py

"""数据查询内容配置"""
# 数据库连接信息
connect_info = 'LRNOP/Inspur*()890@192.168.62.53:1521/SHIRNOP'

# 查询SQL
sql_pkg = '''        select * from db_check t where t.EXECUTE_SDATE >= trunc(sysdate)'''

sql_job = '''        select job, failures, next_date, interval, what from user_jobs j union all ''' \
              '''    select job, failures, next_date, interval, what from user_jobs@wrnop_44 k union all ''' \
              '''    select job, failures, next_date, interval, what from user_jobs@grnop_38 l '''

sql_gather = '''     select * from CSV_CHECKRESULT_HOUR'''

sql_export = '''     select * from TB_EXPORT_VIEW where s_date = trunc(sysdate) and rownum <= 500'''

sql_log = '''        select BLOB_TO_VARCHAR2(binary_output) from SYS.DBA_SCHEDULER_JOB_RUN_DETAILS db where owner = 'LRNOP' 
                     and to_char(db.log_date, 'yyyymmdd') = to_char(sysdate,'yyyymmdd')'''

sql_scheduler = ''' select  job_name, run_count SUCCEED, failure_count as FAILURE,
                    last_run_duration, to_char(next_run_date,'yyyy-mm-dd hh24:mi:ss') next_run_date,
                    repeat_interval, enabled, /*state, comments,*/
                    to_char(last_start_date,'yyyy-mm-dd hh24:mi:ss') last_start_date,
                    job_action
            from dba_scheduler_jobs where OWNER = 'LRNOP' '''

conf_PKG = [connect_info, sql_pkg, 'PKG']
conf_JOB = [connect_info, sql_job, 'JOBS']
conf_GATHER = [connect_info, sql_gather, 'GA']
conf_LOG = [connect_info, sql_log, 'LOG']
conf_SCHEDULER = [connect_info, sql_scheduler, 'SCHEDULER']

