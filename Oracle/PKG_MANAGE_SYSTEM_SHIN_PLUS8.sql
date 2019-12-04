prompt
prompt Creating package PKG_MANAGE_SYSTEM_SHIN_PLUS8
prompt =============================================
prompt
CREATE OR REPLACE PACKAGE PKG_MANAGE_SYSTEM_SHIN_PLUS8 AUTHID CURRENT_USER AS
------------------------------------------------------------------------
  --  OVERVIEW
  --
  --  Oracle 数据库管理与调度通用包
  --
  --  OWNER:    Shinnosuke
  --
  --  VERSION:   3.4
  --
  --  CREATE DATE： 2019/06/11 version 1.0
  --
  --  UPDATE DATE：2019/06/09 version 1.1
  --               1.分区扩展功能--PROC_PARTITION_ADD_RANGE
  --               2.分区删除功能（暂未启用） --PROC_PARTITION_DROP_RANGE 
  --
  --  UPDATE DATE：2019/06/13 version 1.2
  --               1.增加分区清理功能--PROC_PARTITION_CLEANUP_RANGE
  --
  --  UPDATE DATE：2019/06/18 version 1.3
  --               1.增加分区自动扩展功能--PROC_PARTITION_ADD_AUTO
  --               2.增加分区自动清理功能--PROC_PARTITION_CLEANUP_AUTO
  --
  --  UPDATE DATE：2019/06/19 version 1.4
  --               1.修正分区扩展功能，区分天/月
  --
  --  UPDATE DATE：2019/06/21 version 1.5
  --               1.修正分区扩展功能，增加分区表维度入口参数，区分天/周/月
  --               2.修正分区清理功能，增加分区表维度入口参数 ，区分天/周/月
  --               3.增加分区创建功能Demo --TEST1（暂无分区重建功能）
  --  
  --  UPDATE DATE：2019/06/28 version 2.0
  --               1.修正分区自动扩展功能，区分天/周/月
  --               2.修正分区自动清理功能，区分天/周/月
  --
  --  UPDATE DATE：2019/06/28 version 2.1
  --               1.增加分区创建功能Demo --SQL_GET_DDL（三表流转模式保存数据，A → W → B）
  --               2.修正分区创建功能Demo并更名 --SQL_GET_DDL_WK（两表流转模式保存数据，A → WK_A →(rename to) A）
  --               3.修正分区创建功能Demo --SQL_GET_DDL_WK（暂未开启分区重建数据留存功能）
  --
  --  UPDATE DATE：2019/06/28 version 2.2
  --               1.增加删除表闪回功能
  --               2.修正分区清理功能提示
  --               3.修正分区自动清理功能，适配配置表
  --
  --  UPDATE DATE：2019/07/15 version 2.3
  --               1.增加分区清理功能，兼容SYS自增分区表
  --               2.修正分区自动清理功能，兼容SYS_P*自增分区表，适配配置表
  --               3.修正分区自动清理功能，适配配置表
  --
  --  UPDATE DATE：2019/07/16 version 2.4
  --               1.修正分区创建功能，数据回表时关闭logging
  --               2.修正分区自动清理功能，兼容SYS_P*自增分区表，适配配置表，优化遍历流程
  --               3.修正分区扩展功能，周表扩展
  --
  --  UPDATE DATE：2019/07/19 version 2.5
  --               1.修正分区清理功能，Truncate分区，替换Delete操作
  --
  --  UPDATE DATE：2019/07/22 version 2.6
  --               1.新增分区Truncate功能，大数据清理情况下替换Delete操作
  --               2.修正分区自动清理功能，周/月表告警DBMS打印日志
  --
  --  UPDATE DATE：2019/09/07 version 2.7
  --               1.新增简单日志功能 PROC_LOGGING
  --               2.修正分区自动清理功能，多索引表索引分区性判断（包含主键和普通索引）
  --
  --  UPDATE DATE：2019/09/08 version 2.8
  --               1.二次修正分区自动清理功能，多索引表索引分区性判断（非本地/本地主键、非本地/本地普通Index）
  --
  --  UPDATE DATE：2019/10/16 version 3.0
  --               1.增加分区创建功能Demo --SQL_GET_DDL（不保留原始表，产生的WK表在流转过程中替换掉源表）
  --
  --  UPDATE DATE：2019/10/20 version 3.1
  --               1.修正分区自动清理功能（增加分区是否存在判断，分区不存在，跳过）  
  --
  --  UPDATE DATE：2019/10/31 version 3.2
  --               1.新增分区检索过程，按照时间戳 locate 并输出分区名
  --
  --  UPDATE DATE：2019/10/31 version 3.3
  --               1.修正分区自动清理功能（修正索引是否分区判断条件，涵盖主键索引&普通全局索引）
  --
  --  UPDATE DATE：2019/11/07 version 3.4
  --               1.修正分区删除功能（优先修正天级分区清理，按时间自动索引分区名并完成删除）
  --
  --  TODO    1.修正分区自动清理功能，适配标准分区格式，同时适配Oracle系统自增分区表的SYS分区格式（Finished）
  --               2.修正分区删除功能（Pending，优先修正天级，已完成天级）
  --               3.增加分区自动删除功能（Pending）
  --               4.修正分区扩展功能（基于分区表），区分小时  --PROC_PARTITION_ADD_RANGE（Pending）
  --               5.评估表分区重建是否需要添加相关过程，通过临时表方式重建老表并添加分区（Finished）
  --               6.评估是否增加进程相关监控信息（增加了简单的过程监控，确认入库程序是否正常）
  --               7.新增 DBMS_SCHEDULER 调度任务系统任务，可保留并预览LOG
------------------------------------------------------------------------


  --Partition Add Manually（分区名格式化：P_20191028）
  PROCEDURE PROC_PARTITION_ADD_RANGE
  (
    V_TBNAME VARCHAR2, V_TM_GRN VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2,
    V_DATE_THRESHOLD_END VARCHAR2, V_TABLESPACE VARCHAR2
  );
  
  --Partition Delete Manually，非Truncate 
  PROCEDURE PROC_PARTITION_CLEANUP_RANGE
  (V_TBNAME VARCHAR2, V_TM_GRN VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2);

  --分区高速 Partition Truncate Manually
  PROCEDURE PROC_PARTITION_TRUNCATE_RANGE
  (V_TBNAME VARCHAR2, V_TM_GRN VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2);

  --Truncate with Global Index Valid Manually
  PROCEDURE PROC_PARTITION_TRUNCATE_RANGE_INDEX
  (V_TBNAME VARCHAR2, V_TM_GRN VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2);

  --Partition Drop Manually
  PROCEDURE PROC_PARTITION_DROP_RANGE(V_TBNAME VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2);
  
  --Partition Add Automaticly（分区名格式化：P_20191028）
  PROCEDURE PROC_PARTITION_ADD_AUTO;--分区自动扩展
    
  --Partition Truncate Automaticly（分区名格式化：P_20191028）
  PROCEDURE PROC_PARTITION_CLEANUP_AUTO;--分区自动清理
    
  --Partition Locate
  PROCEDURE PROC_PARTITION_LOCATE(V_TABLE_NAME VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_PARTITION_NAME OUT VARCHAR2);

  --TABLE DDL with WK_Table
  PROCEDURE SQL_GET_DDL_WK
  (
    I_FROM_TABLENAME VARCHAR2, I_TM_GRN VARCHAR2, I_FROM_OWNER VARCHAR2,
    I_TABLESPACE VARCHAR2, I_DATE_THRESHOLD_START VARCHAR2
  );
  
  --TABLE DDL without WK_Table
  PROCEDURE SQL_GET_DDL--和 SQL_GET_DDL_WK 的区别在于，不产生中间表，DROP源表，确定情况下使用
  (
    I_FROM_TABLENAME VARCHAR2, I_TM_GRN VARCHAR2, I_FROM_OWNER VARCHAR2,
    I_TABLESPACE VARCHAR2, I_DATE_THRESHOLD_START VARCHAR2
  );
  
  --TABLE Drop Manually
  PROCEDURE DROPTABLE_IFEXISTS(I_TABLE_NAME VARCHAR2, PURGE_FLAG NUMBER);

  --简单的过程 Loggging
  PROCEDURE PROC_LOGGING(i_sdate date, i_pkg_name VARCHAR2,  i_inside_loop_log number,  i_exsit_flag number);
  
  PROCEDURE PROC_TEST;--分区自动扩展


END PKG_MANAGE_SYSTEM_SHIN_PLUS8;
/

prompt
prompt Creating package body PKG_MANAGE_SYSTEM_SHIN_PLUS8
prompt ==================================================
prompt
CREATE OR REPLACE PACKAGE BODY PKG_MANAGE_SYSTEM_SHIN_PLUS8 AS
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
/*  procedure PROC_PARTITION_ADD_RANGE(V_TBNAME varchar2, V_DATE_THRESHOLD_START varchar2, V_DATE_THRESHOLD_END varchar2) is
      --V_TBNAME：待扩展表名
      --V_DATE_THRESHOLD_START：分区扩展起始时间（含起始时间戳）
      --V_DATE_THRESHOLD_END：分区扩展结束时间（含结束时间戳）
      -- v_date date;

      v_date  date;
      v_part_name varchar2(30);
      v_date_name varchar2(200);
      v_loop_log number := 0 ;
      v_tablespace_default varchar2(50);
      BEGIN

          select to_date(V_DATE_THRESHOLD_START,'yyyymmdd') into v_date from dual;--起始时间戳'yyyymmdd'格式化

          select distinct u.tablespace_name into v_tablespace_default
          from \*dba_tables d left join*\ USER_TAB_PARTITIONS u--仅包含分区表
          \*on d.table_name = u.table_name*\
          where u.table_name = V_TBNAME;

          \*select distinct d.owner as i_owner,
          d.table_name as i_tablename,
          decode(u.tablespace_name,null,d.tablespace_name,u.tablespace_name)  as v_tablespace_default,
          to_char(sysdate,'yyyymmdd') as i_part_name,
          to_char(sysdate+1,'yyyy-mm-dd') as i_part_date
          from dba_tables d left join USER_TAB_PARTITIONS u
          on d.table_name = u.table_name
          where d.owner ='LRNOP'
          and (d.table_name like 'ZC%' or d.table_name like'LC_INDEX_LXN_BAD_CELL%')*\

          while v_date >= to_date(V_DATE_THRESHOLD_START,'yyyymmdd') and v_date <= to_date(V_DATE_THRESHOLD_END,'yyyymmdd') LOOP
          --v_loop_log := 1;
          v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');
          v_date_name := ' VALUES LESS THAN (TO_DATE('''|| to_char(v_date+1,'YYYY-MM-DD')||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间
          --样例：execute immediate 'alter table '||V_TBNAME||' drop partition '||v_part_name;
          --样例：ALTER TABLE LRNOP.ZC_CELL_LIST_3G ADD PARTITION P_20190610 VALUES LESS THAN (TO_DATE('2019-06-11 00:00:00','YYYY-MM-DD HH24:MI:SS')) TABLESPACE DBS_D_WRNOP;
          execute immediate 'ALTER TABLE ' || V_TBNAME || ' ADD PARTITION '|| v_part_name || v_date_name ||' TABLESPACE '||v_tablespace_default;--注意拼接空格

          dbms_output.put_line('表'||V_TBNAME||'下的分区'||v_part_name||'已增加！');--执行SQL后输出标记
          v_date := v_date +1;--这里可以再次插入新一轮最小的v_date，进入循环判断
          v_loop_log := v_loop_log +1;
         \* if       then
         else

          end if*\
          end LOOP;
          dbms_output.put_line('本次分区扩展数量: '||v_loop_log||'.');--执行SQL后输出标记
          \*dbms_logmnr.add_logfile( LogFileName => 'E:\TEST1.log.');*\--目前还不能输出缓冲区的文自作为log
      end PROC_PARTITION_ADD_RANGE;*/
      
  PROCEDURE PROC_PARTITION_ADD_RANGE(V_TBNAME VARCHAR2, V_TM_GRN VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2, V_TABLESPACE VARCHAR2) IS
    --V_TBNAME：待扩展表名
    v_date                  date;--起始时间戳
    v_date_partition        date;--分区表的现有最大分区日期
    --v_max_partition_name    varchar2(15);--现有最大分区名
    v_part_name             varchar2(30);--分区名拼接
    v_date_threshold_part   varchar2(10);--当前日期+1 格式化（YYYY-MM-DD）
    v_date_name             varchar2(200);--分区扩展时间段SQL拼接
    v_loop_log              number := 0 ;--计数器
    v_tablespace_default    varchar2(30);--表空间
    BEGIN
                            
        select to_date(V_DATE_THRESHOLD_START,'yyyymmdd') into v_date from dual;--起始时间戳'yyyymmdd'格式化--2019/06/11
        
        --表空间未配置时的默认分配
        if V_TABLESPACE is null 
          then 
          select distinct u.tablespace_name into v_tablespace_default--表空间保存
          from /*dba_tables d left join*/ USER_TAB_PARTITIONS u--仅包含分区表
          /*on d.table_name = u.table_name*/
          where u.table_name = V_TBNAME;
        end if;

        select to_date(regexp_substr(max(u.partition_name),'[^P_]+',1,1,'i'),'yyyymmdd')
        into v_date_partition--现有：2019/06/19（天） /2019/06/17（周） /2019/06/01（月）
        from USER_TAB_PARTITIONS u
        where u.table_name = V_TBNAME;

        /*select max(u.partition_name)
        into v_max_partition_name--现有：P_20190619（天） /P_20190617（周） /P_20190601（月）
        from \*dba_tables d left join*\ USER_TAB_PARTITIONS u--仅包含分区表
        where u.table_name = V_TBNAME;*/

        --分区截止界限非法判断
        if  v_date_partition >= to_date(V_DATE_THRESHOLD_END,'yyyymmdd')--现有分区最大分区日期 >= 截止时间戳，则
            then --奶奶生日--318
            /*RAISE_APPLICATION_ERROR(-20813, '表 '||V_TBNAME||' 下最后一个分区界限为：P_' ||to_char(v_date_partition,'yyyymmdd') ||'. 
            已指定的分区截止界限'||V_DATE_THRESHOLD_END||', 必须调整为高于最后一个分区界限!', TRUE);*/
            dbms_output.put_line('表 '||V_TBNAME||' 下的最后一个分区界限为 P_'||to_char(v_date_partition, 'yyyymmdd')||'！');--执行SQL后输出标记
            return;
            --continue;
            --V_DATE_THRESHOLD_START := to_char(v_date_partition + 1, 'yyyymmdd');--更新循环判断标签
        end if;

        --V_DATE_THRESHOLD_START: 20190611
        --V_DATE_THRESHOLD_END: 20190621
        --v_date：2019/06/11
        --不能直接比大小，需转换为DATE格式
        while /*v_date >= to_date(V_DATE_THRESHOLD_START,'yyyymmdd') and*/ v_date <= to_date(V_DATE_THRESHOLD_END,'yyyymmdd') LOOP
        --v_loop_log := 1;
            if  V_TM_GRN = '2'--天级分区
                then
                  if v_loop_log = 0 then v_date := v_date_partition + 1;--首次进循环，时间=现有最大分区日期+1
                  --v_date：2019/06/20
                  end if;
                  
                  v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');--P_20190620
                  v_date_threshold_part := to_char(v_date + 1,'YYYY-MM-DD');--2019-06-21
                  v_date_name := ' VALUES LESS THAN (TO_DATE('''|| v_date_threshold_part||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间
                  --样例：ALTER TABLE LRNOP.LC_INDEX_LXN_BAD_CELL_DAY_ORC_TEST ADD PARTITION P_20190620 VALUES LESS THAN (TO_DATE('2019-06-21 00:00:00','YYYY-MM-DD HH24:MI:SS')) TABLESPACE DBS_D_WRNOP;
                  if V_TABLESPACE is null 
                    then 
                      execute immediate 'ALTER TABLE ' || V_TBNAME || ' ADD PARTITION '|| v_part_name || v_date_name ||' TABLESPACE '||v_tablespace_default;--注意拼接空格
                    else
                      execute immediate 'ALTER TABLE ' || V_TBNAME || ' ADD PARTITION '|| v_part_name || v_date_name ||' TABLESPACE '||V_TABLESPACE;--注意拼接空格
                  end if;
                  dbms_output.put_line('表 '||V_TBNAME||' 下的天级分区 '||v_part_name||' 已增加！');--执行SQL后输出标记
                  v_date := v_date + 1;--这里将日期更新至次月首日，以判断是否跳出循环 --2019/06/21
                  v_loop_log := v_loop_log + 1;


            elsif  V_TM_GRN = '3'--周级分区
                then
                  if v_loop_log = 0 then v_date := v_date_partition + 7;--2019/06/24;--首次进循环，时间=现有最大分区日期+1
                  --v_date：2019/07/01
                  end if;
                  v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');--P_20190624
                  v_date_threshold_part := to_char(v_date + 7,'YYYY-MM-DD');--2019-07-01
                  v_date_name := ' VALUES LESS THAN (TO_DATE('''|| v_date_threshold_part||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间
                  
                  if V_TABLESPACE is null 
                    then 
                      execute immediate 'ALTER TABLE ' || V_TBNAME || ' ADD PARTITION '|| v_part_name || v_date_name ||' TABLESPACE '||v_tablespace_default;--注意拼接空格
                    else
                      execute immediate 'ALTER TABLE ' || V_TBNAME || ' ADD PARTITION '|| v_part_name || v_date_name ||' TABLESPACE '||V_TABLESPACE;--注意拼接空格
                  end if;                  
                  dbms_output.put_line('表 '||V_TBNAME||' 下的周级分区 '||v_part_name||' 已增加！');--执行SQL后输出标记
                  --v_date := trunc(next_day(v_date,2),'dd');--2019/06/24
                  v_date := v_date + 7;--这里将日期更新至次月首日，以判断是否跳出循环 --2019/06/24
                  v_loop_log := v_loop_log + 1;


            elsif  V_TM_GRN = '4'--月级分区
                then
                  if v_loop_log = 0 then v_date := add_months(v_date_partition,1);--首次进循环，时间=现有最大分区日期+1
                  --v_date：2019/07/01
                  end if;
                  --v_date := add_months(v_date_partition,1);--2019/07/01
                  v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');--P_20190701
                  v_date_threshold_part := to_char(add_months(v_date,1),'YYYY-MM-DD');--2019-08-01
                  v_date_name := ' VALUES LESS THAN (TO_DATE('''|| v_date_threshold_part||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间
                  if V_TABLESPACE is null 
                    then 
                      execute immediate 'ALTER TABLE ' || V_TBNAME || ' ADD PARTITION '|| v_part_name || v_date_name ||' TABLESPACE '||v_tablespace_default;--注意拼接空格
                    else
                      execute immediate 'ALTER TABLE ' || V_TBNAME || ' ADD PARTITION '|| v_part_name || v_date_name ||' TABLESPACE '||V_TABLESPACE;--注意拼接空格
                  end if;        
                  dbms_output.put_line('表 '||V_TBNAME||' 下的月级分区 '||v_part_name||' 已增加！');--执行SQL后输出标记
                  v_date := add_months(v_date,1);--2019/08/01
                  --v_date := trunc(last_day(v_date) + 1,'mm');--这里将日期更新至次月首日，以判断是否跳出循环 --2019/08/01
                  v_loop_log := v_loop_log + 1;
            end if;
        end loop;

        dbms_output.put_line('本次分区扩展数量: '||v_loop_log||'.  完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');--执行SQL后输出标记
        /*dbms_logmnr.add_logfile( LogFileName => 'E:\TEST1.log.');*/--目前还不能输出缓冲区的文自作为log
    
    END PROC_PARTITION_ADD_RANGE;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE PROC_PARTITION_CLEANUP_RANGE(V_TBNAME VARCHAR2, V_TM_GRN VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2) IS
    v_date  date;
    v_part_name varchar2(30);--分区名拼接
    --v_date_threshold_part varchar2(10);--当前日期+1 格式化（YYYY-MM-DD）
   -- v_date_name varchar2(200);--分区扩展时间段SQL拼接
    v_loop_log number := 0 ;--计数器
    --v_tablespace_default varchar2(30);--表空间
    v_partition_type varchar2(20);--分区类型，小新标准分区/系统自增

    c_high_value varchar2(200);
    v_high_value_vc varchar2(20);
    c_table_name varchar2(50);
    c_partition_name varchar2(80);
    --v_ssql varchar2(300);
    v_tb_partition_exists_flag number;

    cursor c_partition_cur is
    select table_name,t.partition_name,t.high_value
    from USER_TAB_PARTITIONS t
    where table_name = V_TBNAME and partition_name not like 'P2%'--SYS_21313
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历

    BEGIN
      
     select count(distinct table_name) into v_tb_partition_exists_flag from user_tables where table_name  = V_TBNAME;
      if v_tb_partition_exists_flag = 0
        then 
             dbms_output.put_line('表 '||V_TBNAME||' 不存在！');
             return;
      end if;
      
      --分区类型判断
      select distinct
      case when partition_name like 'P\_%' escape '\' then 'SHIN'
              when partition_name like 'SYS\_%' escape '\' then 'SYS'
                  else null end partition_type into v_partition_type
      --decode(count(1), 0, to_char(count(1)), max(partition_name) )
      from
      (
          select partition_name
          from USER_TAB_PARTITIONS t
          /* using (table_name) */
          where partition_name not like 'P2%' and table_name = V_TBNAME
      );

      --现有：2019/06/19（天） /2019/06/17（周） /2019/06/01（月）
      select to_date(V_DATE_THRESHOLD_START,'yyyymmdd') into v_date from dual;--起始时间戳'yyyymmdd'格式化--2019/06/20

      while v_date >= to_date(V_DATE_THRESHOLD_START,'yyyymmdd') and v_date <= to_date(V_DATE_THRESHOLD_END,'yyyymmdd') LOOP

        if v_partition_type = 'SHIN'
          then
            --v_loop_log := 1;
            if  V_TM_GRN = '2' --天级分区清理
              then
                v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');
                execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                commit;
                --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190620);
                dbms_output.put_line('表 '||V_TBNAME||' 下的天级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := v_date +1;--这里可以再次插入新一轮最小的v_date，进入循环判断
                v_loop_log := v_loop_log + 1;

            elsif  V_TM_GRN = '3' --月级分区清理
              then
                --样例：3~9/3  10~16/10  17~23/17   24~30/24
                v_date := trunc(v_date, 'iw');--更新 v_date 至起始时间所在周周一的日期，in: 20190620 out: 20190617
                v_part_name := 'P_' ||to_char(v_date, 'yyyymmdd');--P_20190617

                execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190601);
                commit;
                --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||')';
                dbms_output.put_line('表 '||V_TBNAME||' 下的周级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := v_date + 7;--2019/06/24
                --v_date := trunc(last_day(v_date)+1,'mm');--这里将日期更新至次月首日，以判断是否跳出循环 --2019/07/01
                v_loop_log := v_loop_log + 1;

            elsif  V_TM_GRN = '4' --月级分区清理
              then
                v_date := trunc(v_date,'mm');--更新 v_date 至月首日，in: 20190620 out: 20190601
                v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');

                execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190601);
                commit;
                --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||')';
                dbms_output.put_line('表 '||V_TBNAME||' 下的月级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := add_months(v_date,1);--2019/08/01
                --v_date := trunc(last_day(v_date)+1,'mm');--这里将日期更新至次月首日，以判断是否跳出循环 --2019/07/01
                v_loop_log := v_loop_log + 1;
            end if;

        elsif v_partition_type = 'SYS'
          then
            begin
              open c_partition_cur;--字典表内获取非标准分区的分区名
                  loop--******
                      fetch c_partition_cur into c_table_name ,c_partition_name, c_high_value;
                      exit when NOT c_partition_cur%FOUND;
                      v_high_value_vc := substr(c_high_value, 11, 10); --less than 2019-07-14 ...
                      if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') between V_DATE_THRESHOLD_START and V_DATE_THRESHOLD_END)
                      --and c_table_name = V_TBNAME--只索引SYS分区表
                          then
                            execute immediate 'DELETE FROM '||V_TBNAME||' PARTITION('||c_partition_name||')';
                            --v_ssql := 'DELETE FROM '||V_TBNAME||' PARTITION('||c_partition_name||')';
                            --execute immediate v_ssql;--******
                            commit;
                            --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||c_partition_name||')';
                            dbms_output.put_line('表 '||V_TBNAME||' 下的SYS自动分区 '||c_partition_name||' 已清理！');--执行SQL后输出标记
                            v_loop_log := v_loop_log + 1;
                            if to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START
                              then
                                 --SYS系统自增表在进入遍历后便会完成全部时间的遍历，无需再激活外层时间LOOP，故v_date = 截止时间 + 1
                                 v_date := to_date(V_DATE_THRESHOLD_END,'yyyymmdd') + 1;
                                 exit;--降序遍历，达到遍历下限时，任务完成，退出！！！抛弃多余遍历
                            --else continue;
                            end if;
                      end if;
                  --fetch c_partition_cur into tab_owner,tab_name,tab_partition,tab_high_value;
                  end loop;
              close c_partition_cur;
            end;

        end if;
      END LOOP;
      dbms_output.put_line('表'||V_TBNAME||' 下的分区清理数量合计: '||v_loop_log||'.  完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');
      dbms_output.put_line('----------------------------------------------------------------------');
      /*dbms_logmnr.add_logfile( LogFileName => 'E:\TEST1.log.');*/--目前还不能输出缓冲区的文自作为log

    END PROC_PARTITION_CLEANUP_RANGE;

  /*PROCEDURE PROC_PARTITION_CLEANUP_RANGE(V_TBNAME VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2) IS
      -- v_date date;
      \*v_date_threshold varchar(10);
      V_TBNAME varchar2(50);  *\
      v_date  date;
      v_part_name varchar2(30);
      v_loop_log number := 0 ;
      BEGIN
          -- v_date := date'2015-2-4';
          -- select to_date(20180901,'yyyymmdd') into v_date from dual;
          \*select to_char(trunc(to_date(start_date,'YYYYMMDD'),'MM'\*返回当月首日*\),'YYYYMMDD')into date_mth_start from dual;*\
          \*select date_threshold into v_date_threshold from dual;
          select tbname into V_TBNAME from dual;*\
          \*select to_date(min(partition_name),'yyyymmdd')into v_date
          from
          (
              select distinct \*t.table_name,*\ to_number(substr(t.partition_name,3)) as partition_name
              from SYS.USER_TAB_PARTITIONS t where  t.table_name = V_TBNAME
              \*t.partition_name like 'P_201808%' *\
              --and t.table_name not like '%$%' --and t.num_rows is not null;
              --order by\* t.table_name,*\partition_name
          );*\

          select to_date(V_DATE_THRESHOLD_START,'yyyymmdd') into v_date from dual;

          while v_date >= to_date(V_DATE_THRESHOLD_START,'yyyymmdd') and v_date <= to_date(V_DATE_THRESHOLD_END,'yyyymmdd') LOOP
          --v_loop_log := 1;
          v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');
          execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
          commit;
          dbms_output.put_line('表'||V_TBNAME||'下的分区'||v_part_name||'已清理！');--执行SQL后输出标记

          v_date := v_date +1;--这里可以再次插入新一轮最小的v_date，进入循环判断
          v_loop_log := v_loop_log +1;
         \* if       then
         else

          end if*\
          end LOOP;
          dbms_output.put_line('本次分区清理数量: '||v_loop_log||'.');--执行SQL后输出标记
          \*dbms_logmnr.add_logfile( LogFileName => 'E:\TEST1.log.');*\--目前还不能输出缓冲区的文自作为log
      END PROC_PARTITION_CLEANUP_RANGE;*/

  /*PROCEDURE PROC_PARTITION_CLEANUP_RANGE(V_TBNAME VARCHAR2, V_TM_GRN VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2) IS
    v_date  date;
    v_part_name varchar2(30);--分区名拼接
    --v_date_threshold_part varchar2(10);--当前日期+1 格式化（YYYY-MM-DD）
   -- v_date_name varchar2(200);--分区扩展时间段SQL拼接
    v_loop_log number := 0 ;--计数器
    --v_tablespace_default varchar2(30);--表空间
    begin
        \*select to_char(trunc(to_date(start_date,'YYYYMMDD'),'MM'\*返回当月首日*\),'YYYYMMDD')into date_mth_start from dual;*\
        \*select date_threshold into v_date_threshold from dual;
        select tbname into V_TBNAME from dual;*\
        
        
        --现有：2019/06/19（天） /2019/06/17（周） /2019/06/01（月）
        select to_date(V_DATE_THRESHOLD_START,'yyyymmdd') into v_date from dual;--起始时间戳'yyyymmdd'格式化--2019/06/20

        while v_date >= to_date(V_DATE_THRESHOLD_START,'yyyymmdd') and v_date <= to_date(V_DATE_THRESHOLD_END,'yyyymmdd') LOOP
            --v_loop_log := 1;
            if  V_TM_GRN = '2' --天级分区清理
                then
                v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');
                execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190620);
                commit;
                dbms_output.put_line('表 '||V_TBNAME||' 下的天级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := v_date +1;--这里可以再次插入新一轮最小的v_date，进入循环判断
                v_loop_log := v_loop_log +1;
                
            elsif  V_TM_GRN = '3' --月级分区清理
                then
                --样例：3~9/3  10~16/10  17~23/17   24~30/24
                v_date := trunc(v_date, 'iw');--更新 v_date 至起始时间所在周周一的日期，in: 20190620 out: 20190617
                v_part_name := 'P_' ||to_char(v_date, 'yyyymmdd');--P_20190617

                execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190601);
                commit;
                dbms_output.put_line('表 '||V_TBNAME||' 下的周级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := v_date + 7;--2019/06/24
                --v_date := trunc(last_day(v_date)+1,'mm');--这里将日期更新至次月首日，以判断是否跳出循环 --2019/07/01
                v_loop_log := v_loop_log +1;

            elsif  V_TM_GRN = '4' --月级分区清理
                then
                v_date := trunc(v_date,'mm');--更新 v_date 至月首日，in: 20190620 out: 20190601
                v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');

                execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190601);
                commit;
                dbms_output.put_line('表 '||V_TBNAME||' 下的月级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := add_months(v_date,1);--2019/08/01
                --v_date := trunc(last_day(v_date)+1,'mm');--这里将日期更新至次月首日，以判断是否跳出循环 --2019/07/01
                v_loop_log := v_loop_log +1;
            end if;

        end LOOP;
        dbms_output.put_line('表'||V_TBNAME||'下的分区清理数量合计: '||v_loop_log||'.  完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');--执行SQL后输出标记
        \*dbms_logmnr.add_logfile( LogFileName => 'E:\TEST1.log.');*\--目前还不能输出缓冲区的文自作为log
        
    END PROC_PARTITION_CLEANUP_RANGE;*/

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE PROC_PARTITION_TRUNCATE_RANGE(V_TBNAME VARCHAR2, V_TM_GRN VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2) IS
    v_date  date;
    v_part_name varchar2(30);--分区名拼接
    --v_date_threshold_part varchar2(10);--当前日期+1 格式化（YYYY-MM-DD）
   -- v_date_name varchar2(200);--分区扩展时间段SQL拼接
    v_loop_log number := 0 ;--计数器
    --v_tablespace_default varchar2(30);--表空间
    v_partition_type varchar2(20);--分区类型，小新标准分区/系统自增

    c_high_value varchar2(200);
    v_high_value_vc varchar2(20);
    c_table_name varchar2(50);
    c_partition_name varchar2(80);
    --v_ssql varchar2(300);
    v_tb_partition_exists_flag number;

    cursor c_partition_cur is
    select table_name,t.partition_name,t.high_value
    from USER_TAB_PARTITIONS t
    where table_name = V_TBNAME and partition_name not like 'P2%'--SYS_21313
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历

    BEGIN

     select count(distinct table_name) into v_tb_partition_exists_flag from user_tables where table_name  = V_TBNAME;
      if v_tb_partition_exists_flag = 0
        then 
             dbms_output.put_line('表 '||V_TBNAME||' 不存在！');
             return;
      end if;

      --分区类型判断
      select distinct
      case when partition_name like 'P\_%' escape '\' then 'SHIN'
              when partition_name like 'SYS\_%' escape '\' then 'SYS'
                  else null end partition_type into v_partition_type
      --decode(count(1), 0, to_char(count(1)), max(partition_name) )
      from
      (
          select partition_name, rownum rn
          from USER_TAB_PARTITIONS t
          /* using (table_name) */
          where partition_name not like 'P2%' and table_name = V_TBNAME
      )where rn >= 5;

      --现有：2019/06/19（天） /2019/06/17（周） /2019/06/01（月）
      select to_date(V_DATE_THRESHOLD_START,'yyyymmdd') into v_date from dual;--起始时间戳'yyyymmdd'格式化--2019/06/20

      while v_date >= to_date(V_DATE_THRESHOLD_START,'yyyymmdd') and v_date <= to_date(V_DATE_THRESHOLD_END,'yyyymmdd') LOOP

        if v_partition_type = 'SHIN'
          then
            --v_loop_log := 1;
            if  V_TM_GRN = '2' --天级分区清理
              then
                v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');
                /*execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                commit;*/
                --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||') UPDATE GLOBAL INDEXES';
                execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190620);
                dbms_output.put_line('表 '||V_TBNAME||' 下的天级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := v_date +1;--这里可以再次插入新一轮最小的v_date，进入循环判断
                v_loop_log := v_loop_log + 1;

            elsif  V_TM_GRN = '3' --月级分区清理
              then
                --样例：3~9/3  10~16/10  17~23/17   24~30/24
                v_date := trunc(v_date, 'iw');--更新 v_date 至起始时间所在周周一的日期，in: 20190620 out: 20190617
                v_part_name := 'P_' ||to_char(v_date, 'yyyymmdd');--P_20190617

                /*execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190601);
                commit;*/
                --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||') UPDATE GLOBAL INDEXES';
                execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||')';
                dbms_output.put_line('表 '||V_TBNAME||' 下的周级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := v_date + 7;--2019/06/24
                --v_date := trunc(last_day(v_date)+1,'mm');--这里将日期更新至次月首日，以判断是否跳出循环 --2019/07/01
                v_loop_log := v_loop_log + 1;

            elsif  V_TM_GRN = '4' --月级分区清理
              then
                v_date := trunc(v_date,'mm');--更新 v_date 至月首日，in: 20190620 out: 20190601
                v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');

                /*execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190601);
                commit;*/
                --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||') UPDATE GLOBAL INDEXES';
                execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||')';
                dbms_output.put_line('表 '||V_TBNAME||' 下的月级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := add_months(v_date,1);--2019/08/01
                --v_date := trunc(last_day(v_date)+1,'mm');--这里将日期更新至次月首日，以判断是否跳出循环 --2019/07/01
                v_loop_log := v_loop_log + 1;
            end if;

        elsif v_partition_type = 'SYS'
          then
            begin
              open c_partition_cur;--字典表内获取非标准分区的分区名
                  loop--******
                      fetch c_partition_cur into c_table_name ,c_partition_name, c_high_value;
                      exit when NOT c_partition_cur%FOUND;
                      if substr(c_high_value, 1, 2) = 'TI'--TIMESTAMP' 2019-04-25 00:00:00'
                          then v_high_value_vc := substr(c_high_value, 12, 10);
                      else
                          v_high_value_vc := substr(c_high_value, 11, 10); --less than 2019-07-14 ...
                      end if;
                      if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') between V_DATE_THRESHOLD_START and V_DATE_THRESHOLD_END)
                      --and c_table_name = V_TBNAME--只索引SYS分区表
                          then
                            /*execute immediate 'DELETE FROM '||V_TBNAME||' PARTITION('||c_partition_name||')';
                            --v_ssql := 'DELETE FROM '||V_TBNAME||' PARTITION('||c_partition_name||')';
                            --execute immediate v_ssql;--******
                            commit;*/
                            --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||c_partition_name||') UPDATE GLOBAL INDEXES';
                            execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||c_partition_name||')';
                            dbms_output.put_line('表 '||V_TBNAME||' 下的SYS自动分区 '||c_partition_name||' 已清理！');--执行SQL后输出标记
                            v_loop_log := v_loop_log + 1;
                            if to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START
                              then
                                 --SYS系统自增表在进入遍历后便会完成全部时间的遍历，无需再激活外层时间LOOP，故v_date = 截止时间 + 1
                                 v_date := to_date(V_DATE_THRESHOLD_END,'yyyymmdd') + 1;
                                 exit;--降序遍历，达到遍历下限时，任务完成，退出！！！抛弃多余遍历
                            --else continue;
                            end if;
                      end if;
                  --fetch c_partition_cur into tab_owner,tab_name,tab_partition,tab_high_value;
                  end loop;
              close c_partition_cur;
            end;

        end if;
      END LOOP;
      dbms_output.put_line('表 '||V_TBNAME||' 下的分区清理数量合计: '||v_loop_log||'.  完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');
      dbms_output.put_line('----------------------------------------------------------------------');
      /*dbms_logmnr.add_logfile( LogFileName => 'E:\TEST1.log.');*/--目前还不能输出缓冲区的文自作为log

    END PROC_PARTITION_TRUNCATE_RANGE;

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE PROC_PARTITION_TRUNCATE_RANGE_INDEX(V_TBNAME VARCHAR2, V_TM_GRN VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2) IS
    v_date  date;
    v_part_name varchar2(30);--分区名拼接
    --v_date_threshold_part varchar2(10);--当前日期+1 格式化（YYYY-MM-DD）
   -- v_date_name varchar2(200);--分区扩展时间段SQL拼接
    v_loop_log number := 0 ;--计数器
    --v_tablespace_default varchar2(30);--表空间
    v_partition_type varchar2(20);--分区类型，小新标准分区/系统自增

    c_high_value varchar2(200);
    v_high_value_vc varchar2(20);
    c_table_name varchar2(50);
    c_partition_name varchar2(80);
    --v_ssql varchar2(300);
    v_tb_partition_exists_flag number;

    cursor c_partition_cur is
    select table_name,t.partition_name,t.high_value
    from USER_TAB_PARTITIONS t
    where table_name = V_TBNAME and partition_name not like 'P2%'--SYS_21313
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历

    BEGIN

     select count(distinct table_name) into v_tb_partition_exists_flag from user_tables where table_name  = V_TBNAME;
      if v_tb_partition_exists_flag = 0
        then 
             dbms_output.put_line('表 '||V_TBNAME||' 不存在！');
             return;
      end if;

      --分区类型判断
      select distinct
      case when partition_name like 'P\_%' escape '\' then 'SHIN'
              when partition_name like 'SYS\_%' escape '\' then 'SYS'
                  else null end partition_type into v_partition_type
      --decode(count(1), 0, to_char(count(1)), max(partition_name) )
      from
      (
          select partition_name, rownum rn
          from USER_TAB_PARTITIONS t
          /* using (table_name) */
          where partition_name not like 'P2%' and table_name = V_TBNAME
      )where rn >= 5;

      --现有：2019/06/19（天） /2019/06/17（周） /2019/06/01（月）
      select to_date(V_DATE_THRESHOLD_START,'yyyymmdd') into v_date from dual;--起始时间戳'yyyymmdd'格式化--2019/06/20

      while v_date >= to_date(V_DATE_THRESHOLD_START,'yyyymmdd') and v_date <= to_date(V_DATE_THRESHOLD_END,'yyyymmdd') LOOP

        if v_partition_type = 'SHIN'
          then
            --v_loop_log := 1;
            if  V_TM_GRN = '2' --天级分区清理
              then
                v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');
                /*execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                commit;*/
                execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||') UPDATE GLOBAL INDEXES';
                -- execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190620);
                dbms_output.put_line('表 '||V_TBNAME||' 下的天级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := v_date +1;--这里可以再次插入新一轮最小的v_date，进入循环判断
                v_loop_log := v_loop_log + 1;

            elsif  V_TM_GRN = '3' --月级分区清理
              then
                --样例：3~9/3  10~16/10  17~23/17   24~30/24
                v_date := trunc(v_date, 'iw');--更新 v_date 至起始时间所在周周一的日期，in: 20190620 out: 20190617
                v_part_name := 'P_' ||to_char(v_date, 'yyyymmdd');--P_20190617

                /*execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190601);
                commit;*/
                execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||') UPDATE GLOBAL INDEXES';
                --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||')';
                dbms_output.put_line('表 '||V_TBNAME||' 下的周级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := v_date + 7;--2019/06/24
                --v_date := trunc(last_day(v_date)+1,'mm');--这里将日期更新至次月首日，以判断是否跳出循环 --2019/07/01
                v_loop_log := v_loop_log + 1;

            elsif  V_TM_GRN = '4' --月级分区清理
              then
                v_date := trunc(v_date,'mm');--更新 v_date 至月首日，in: 20190620 out: 20190601
                v_part_name := 'P_' ||to_char(v_date,'yyyymmdd');

                /*execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_part_name||')';
                --样例：execute immediate 'DELETE FROM XXX partition(P_20190601);
                commit;*/
                execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||') UPDATE GLOBAL INDEXES';
                --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||v_part_name||')';
                dbms_output.put_line('表 '||V_TBNAME||' 下的月级分区 '||v_part_name||' 已清理！');--执行SQL后输出标记
                v_date := add_months(v_date,1);--2019/08/01
                --v_date := trunc(last_day(v_date)+1,'mm');--这里将日期更新至次月首日，以判断是否跳出循环 --2019/07/01
                v_loop_log := v_loop_log + 1;
            end if;

        elsif v_partition_type = 'SYS'
          then
            begin
              open c_partition_cur;--字典表内获取非标准分区的分区名
                  loop--******
                      fetch c_partition_cur into c_table_name ,c_partition_name, c_high_value;
                      exit when NOT c_partition_cur%FOUND;
                      if substr(c_high_value, 1, 2) = 'TI'--TIMESTAMP' 2019-04-25 00:00:00'
                          then v_high_value_vc := substr(c_high_value, 12, 10);
                      else
                          v_high_value_vc := substr(c_high_value, 11, 10); --less than 2019-07-14 ...
                      end if;
                      if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') between V_DATE_THRESHOLD_START and V_DATE_THRESHOLD_END)
                      --and c_table_name = V_TBNAME--只索引SYS分区表
                          then
                            /*execute immediate 'DELETE FROM '||V_TBNAME||' PARTITION('||c_partition_name||')';
                            --v_ssql := 'DELETE FROM '||V_TBNAME||' PARTITION('||c_partition_name||')';
                            --execute immediate v_ssql;--******
                            commit;*/
                            execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||c_partition_name||') UPDATE GLOBAL INDEXES';
                            --execute immediate 'ALTER TABLE '||V_TBNAME||' TRUNCATE PARTITION('||c_partition_name||')';
                            dbms_output.put_line('表 '||V_TBNAME||' 下的SYS自动分区 '||c_partition_name||' 已清理！');--执行SQL后输出标记
                            v_loop_log := v_loop_log + 1;
                            if to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START
                              then
                                 --SYS系统自增表在进入遍历后便会完成全部时间的遍历，无需再激活外层时间LOOP，故v_date = 截止时间 + 1
                                 v_date := to_date(V_DATE_THRESHOLD_END,'yyyymmdd') + 1;
                                 exit;--降序遍历，达到遍历下限时，任务完成，退出！！！抛弃多余遍历
                            --else continue;
                            end if;
                      end if;
                  --fetch c_partition_cur into tab_owner,tab_name,tab_partition,tab_high_value;
                  end loop;
              close c_partition_cur;
            end;

        end if;
      END LOOP;
      dbms_output.put_line('表 '||V_TBNAME||' 下的分区清理数量合计: '||v_loop_log||'.  完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');
      dbms_output.put_line('----------------------------------------------------------------------');
      /*dbms_logmnr.add_logfile( LogFileName => 'E:\TEST1.log.');*/--目前还不能输出缓冲区的文自作为log

    END PROC_PARTITION_TRUNCATE_RANGE_INDEX;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE PROC_PARTITION_DROP_RANGE(V_TBNAME VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_DATE_THRESHOLD_END VARCHAR2) IS
    v_partition_name                varchar2(50);
    v_date  date;
    --v_part_name varchar2(30);
    v_part_name_drop varchar2(30);
    v_loop_log number := 0 ;
    --v_tb_partition_exists_flag number;
    
    BEGIN
        -------
        v_date := to_date(V_DATE_THRESHOLD_START,'yyyymmdd'); --20191107 --> 2019-11-07

        WHILE v_date >= to_date(V_DATE_THRESHOLD_START,'yyyymmdd') and v_date <= to_date(V_DATE_THRESHOLD_END,'yyyymmdd') LOOP
        --v_loop_log := 1;
            v_part_name_drop := to_char(v_date, 'yyyymmdd'); --20191107
            PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, v_part_name_drop , v_partition_name);

            if v_partition_name = 'NULL'
               then
                   dbms_output.put_line('表 '||V_TBNAME||' 下对应时间 '||v_part_name_drop||' 的分区不存在！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');
                   v_date := v_date +1;--这里可以再次插入新一轮最小的v_date，进入循环判断                   
                   continue;
            else
                   --已标记 20191107 对应的系统自动分区 SYS_XXX 或 P_20191107
                   execute immediate 'ALTER TABLE '||V_TBNAME||' DROP PARTITION ('||v_partition_name ||') UPDATE GLOBAL INDEXES'; 
                   dbms_output.put_line('表'||V_TBNAME||'下的分区 '||v_partition_name||' 已删除！');--执行SQL后输出标记
                   v_date := v_date +1;--这里可以再次插入新一轮最小的v_date，进入循环判断
                   v_loop_log := v_loop_log +1;
            end if;
            
        END LOOP;
        dbms_output.put_line('本次分区删除数量: '||v_loop_log||'. 完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');--执行SQL后输出标记
        /*dbms_logmnr.add_logfile( LogFileName => 'E:\TEST1.log.');*/--目前还不能输出缓冲区的文自作为log
    END PROC_PARTITION_DROP_RANGE;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE PROC_PARTITION_ADD_AUTO IS
    --Editor：Shinnosuke
    --Updating：2019/05/07
    --天级表分区&月级表分区每天进行判断，天级表按日自增，月级表每月月底自增至下月！！！

    --Updating：2019/05/08
    --1.月级表分区扩展判断条件修正！！！
    --2.月级表分区扩展注释修正！！！

    ---------------------------------------
      ssql varchar2(4000);
      cursor C_JOB   is
      select sql_string from--这里的next_day可行，是因为这里的SQL为动态DDL拼接语句，非单纯的判断条件，被紧跟执行内容！！！
      (
            select
            case
               when s.tm_grn = '2'--每天执行天级表扩展，表分区扩展至sysdate+1
                    then 'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_ADD_RANGE('''|| table_name||''','''|| tm_grn ||''','''||to_char(sysdate+1,'yyyymmdd')||''','''||to_char(sysdate+1,'yyyymmdd')||''','''||TABLESPACE_NAME ||''')'
               when s.tm_grn = '3'--每周周三执行，表分区扩展至次周周一
               and trunc(sysdate) = trunc(next_day(sysdate,4))-7
               --and trunc(sysdate,'dd') = last_day(trunc(sysdate, 'mm'))
               --and to_char(sysdate,'yyyymmdd') = '20190626'--测试时间戳
                    then 'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_ADD_RANGE('''|| table_name||''','''|| tm_grn ||''','''||to_char(trunc(next_day(sysdate,4)),'yyyymmdd')||''','''||to_char(trunc(next_day(sysdate,4)),'yyyymmdd')||''','''||TABLESPACE_NAME ||''')'
               when s.tm_grn = '4'--每月月底前3天，执行月级表扩展，表分区扩展至次月首日
               and trunc(sysdate) = last_day(trunc(sysdate, 'mm'))-3
                    then 'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_ADD_RANGE('''|| table_name||''','''|| tm_grn ||''','''||to_char(last_day(trunc(sysdate, 'mm'))+1,'yyyymmdd')||''','''||to_char(last_day(trunc(sysdate, 'mm'))+1,'yyyymmdd')||''','''||TABLESPACE_NAME ||''')'
            end as sql_string--拼接分区扩展语句
            from FAST_DATA_PROCESS_TEMPLATE s
            where s.activate_flag = 1
            order by template_id
      )
      where sql_string is not null;
      --定义游标变量 c_row
      c_row c_job%rowtype;--定义行对象
      i number;
      --v_cursor number;
      --v_row number;--行数
      BEGIN
          --SQL_LINE := t_sql_table();
          i := 1;
          open c_job;
            loop
                fetch c_job into c_row;
                exit when c_job%notfound;
                --SQL_LINE.Extend(1);
                --v_cursor:=dbms_sql.open_cursor;
                ssql := c_row.sql_string;
                --dbms_sql.parse(v_cursor,ssql,dbms_sql.native); --分析语句
                --v_row:=dbms_sql.execute(v_cursor); --执行语句
                --dbms_sql.close_cursor(v_cursor); --关闭光标
                execute immediate ssql;
                dbms_output.put_line('SQL_LINE_'||i|| ' ：已执行！执行内容：'||ssql||'.
                ');--执行SQL后输出标记
                i :=i+1;
            end loop;
          close c_job;
          --return SQL_LINE;
      END PROC_PARTITION_ADD_AUTO;
     

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE PROC_PARTITION_CLEANUP_AUTO IS
    V_TBNAME varchar2(50);--:= 'LC_INDEX_LXN_BAD_CELL_DAY_ORC_TEST'; --待扩展表名
    --V_DATE_THRESHOLD_START varchar2(10);--分区扩展起始时间（含起始时间戳）
    --v_date_threshold_part varchar2(10);--分区扩展结束时间（含结束时间戳）
    --v_date_start  date;--当前日期
    --v_date_start_clean date;--清理起始时间戳
    v_date_start_clean_vc varchar2(15);--清理起始时间格式化（yyyymmdd）
    v_partitioned varchar2(15);--待清理分区名
    v_tm_grn varchar2(5);
    --v_template_id number :=0;--配置表内表格的唯一标识
    --v_template_id_max number;--配置表内唯一标识上限
    v_rownum number := 0;--
    v_rownum_max number;--
    --v_table_name varchar2(100);
    v_cleanup_flag number;--配置表内激活标识
    v_ssql varchar2(1000);
    --v_day_clean number;
    --v_high_value_vc                 varchar2(20);
    v_partition_name                varchar2(50);
    v_partition_type varchar2(20);--分区类型，小新标准分区/系统自增
    v_tb_partition_exists_flag number;
    v_tb_exists_flag number;
    v_index_partition_status varchar2(30);
    v_date_start_noclean_vc varchar2(8) := to_char(trunc(sysdate, 'mm') + 14, 'yyyymmdd'); --Freq=Daily without 15th every month


    /*cursor cur_partition is --字典表内获取非标准分区的分区名
    select table_name,t.partition_name,t.high_value
    from USER_TAB_PARTITIONS t
    where table_name = V_TBNAME and partition_name not like 'P2%'--SYS_21313
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历

    type partition_type is record(
        table_name      varchar2(200),
        partition_name  varchar2(50),
        high_value      varchar2(100)
    );
    partition_tmp partition_type;*/

    BEGIN
        --遍历
        --方法一：通过 template_id 进行循环遍历，缺点：template_id 不连续的部分会造成程序报错！！！
        --select max(template_id) into v_template_id_max from FAST_DATA_PROCESS_TEMPLATE;
        --方法二：通过 rownum 进行循环遍历，缺点：遍历的顺序非各表的执行顺序，程序不会报错！！！
        select max(rownum) into v_rownum_max from FAST_DATA_PROCESS_TEMPLATE;
        --PLUS7暂未部署
        --PKG_MANAGE_SYSTEM_PLUS7_0.PROC_PARTITION_CLEANUP_RANGE('LRNOP','LC_INDEX_LXN_BAD_CELL_DAY',v_date_start,v_date_start,'0','0');


        --自动清理冗余数据
        --while v_template_id < v_template_id_max loop--遍历配置表内所有表
        while v_rownum < v_rownum_max loop--******
            v_rownum := v_rownum + 1;
            --按照配置表的Template_id顺序，遍历
            /*select table_name into V_TBNAME from
            (
                select table_name, rownum as rn from
                (
                    select s.table_name from FAST_DATA_PROCESS_TEMPLATE s order by s.template_id
                )t
            )where rn = v_rownum;*/
            execute immediate 
            'select table_name from
            (
                select table_name, rownum as rn from
                (
                    select s.table_name from FAST_DATA_PROCESS_TEMPLATE s order by s.template_id
                )t
            )where rn = :v_rownum' into V_TBNAME using v_rownum;
                        
            /*V_TBNAME := 'PERF_CELL_L_3';*/
            --判断表是否存在
            /*select count(distinct table_name) into v_tb_exists_flag from user_tables where table_name  = V_TBNAME;*/
            execute immediate 'select count(distinct table_name) from user_tables where table_name = :V_TBNAME'into v_tb_exists_flag using V_TBNAME;
            
            --获取表的清理激活状态
            /*select s.cleanup_flag  into v_cleanup_flag from FAST_DATA_PROCESS_TEMPLATE s where s.table_name = V_TBNAME;--where s.template_id = v_template_id;*/
            execute immediate 'select s.cleanup_flag from FAST_DATA_PROCESS_TEMPLATE s where s.table_name = :V_TBNAME'
            into v_cleanup_flag using V_TBNAME;
            --dbms_output.put_line(V_TBNAME);
            
            --若表不存在或分区清理未激活，跳出，激活下一轮循环
            --分区不存在类型，在后续流程中判断
            if v_tb_exists_flag = 0
               then
                    dbms_output.put_line('表 '||V_TBNAME||' 不存在！');
                    continue;
            elsif v_tb_exists_flag <> 0 and (v_cleanup_flag = 0 or v_cleanup_flag is null)
               then
                    dbms_output.put_line('表 '||V_TBNAME||' 的自动清理权限未开启！');
                    continue;
            else
               --从配置表中获取表维度
               /*select s.tm_grn into v_tm_grn from  FAST_DATA_PROCESS_TEMPLATE s where s.table_name = V_TBNAME;*/
               execute immediate 'select s.tm_grn from  FAST_DATA_PROCESS_TEMPLATE s where s.table_name = :V_TBNAME'into v_tm_grn using V_TBNAME;
               
               if v_tm_grn is null
                   then
                        dbms_output.put_line('配置表中表 '||V_TBNAME||' 的时间维度字段为空，请修正！');
                        continue;
               else
                   --获取清理间隔
                   --从配置表中获取清理时间范围，计算出清理时间戳
                   execute immediate 
                   'select decode
                   (
                     cleanup_day_range, null,
                     (case :v_tm_grn when ''2'' then to_char((trunc(sysdate) - 60), ''yyyymmdd'')--20190328
                                              when ''3'' then to_char(trunc(next_day((trunc(sysdate) - 60),''星期一''))-7,''yyyymmdd'')
                                                 when ''4'' then to_char(trunc((trunc(sysdate) - 60),''mm''),''yyyymmdd'')end
                     ),
                     (case :v_tm_grn when ''2'' then to_char((trunc(sysdate) - s.cleanup_day_range), ''yyyymmdd'')--20190328
                                              when ''3'' then to_char(trunc(next_day((trunc(sysdate) - s.cleanup_day_range),''星期一''))-7,''yyyymmdd'')
                                                 when ''4'' then to_char(trunc((trunc(sysdate) - s.cleanup_day_range),''mm''),''yyyymmdd'') end
                     )
                   )
                   from FAST_DATA_PROCESS_TEMPLATE s where s.table_name = :V_TBNAME' into v_date_start_clean_vc using v_tm_grn, v_tm_grn, V_TBNAME;
               end if;
               
               --20191106 ZYJ用户天表只每月保留单日数据（目标时间戳：15th）
               if V_TBNAME = 'ZYJ_MR_USER_CELL' and v_date_start_clean_vc = v_date_start_noclean_vc
                   then
                        dbms_output.put_line('时间戳：'||v_date_start_clean_vc||', 表 '||V_TBNAME||' 的周期清理跳过！');
                        continue;
               end if;
               
               --判断是否分区
               execute immediate 'select partitioned from user_tables where table_name = :V_TBNAME' into v_partitioned using V_TBNAME;
               
               --判断分区类型，排除首分区
               execute immediate
               'select partition_type from
               (
                 select distinct
                        case when partition_name like ''P\_%'' escape ''\'' then ''SHIN''
                                  when partition_name like ''SYS\_%'' escape ''\'' then ''SYS''
                                       else ''NULL'' end partition_type
                 --decode(count(1), 0, to_char(count(1)), max(partition_name) )
                 from
                 (
                    select partition_name from
                    (select * from FAST_DATA_PROCESS_TEMPLATE s where s.table_name = :V_TBNAME)s
                    left join USER_TAB_PARTITIONS t
                    using (table_name)
                    where partition_name not like ''P2%''--规避分区头判断结果为NULL的情况
                 )
               )where partition_type is not null' into v_partition_type using V_TBNAME;
               
               --20191018
               /*if v_partition_type = 'SHIN'
                 then--标准分区，则判断指定分区是否存在
                     execute immediate
                     'select count(partition_name) --u.partition_name
                     from USER_TAB_PARTITIONS u
                     where u.table_name = :V_TBNAME
                     and substr(u.partition_name,3) = :partition_date' into v_tb_partition_exists_flag
                     using V_TBNAME, v_date_start_clean_vc;
               elsif v_partition_type = 'SYS'
                 then
                     v_tb_partition_exists_flag := 1; --默认SYS系统分区存在
               end if;*/
               
               v_partition_name := '';
               
               /*open cur_partition; --开始索引字典表
               fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
               loop--------------
                     exit when NOT (cur_partition%FOUND);
                     if substr(partition_tmp.high_value, 1, 2) = 'TI'--TIMESTAMP' 2019-04-25 00:00:00'
                         then v_high_value_vc := substr(partition_tmp.high_value, 12, 10);
                     else
                         v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
                     end if;
                     if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = v_date_start_clean_vc)
                        then
                           v_partition_name := partition_tmp.partition_name;
                           dbms_output.put_line(v_partition_name);
                           exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
                     end if;
                     fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
               end loop;
               close cur_partition;--------------*/
               
               PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, v_date_start_clean_vc, v_partition_name);
               
               if v_partition_name = 'NULL'
                 then
                     v_tb_partition_exists_flag := 0;
               else 
                     v_tb_partition_exists_flag := 1;
               end if;
               
               
               if v_tb_partition_exists_flag = 0
                 then
                     dbms_output.put_line('表 '||V_TBNAME||' 下对应时间 '||v_date_start_clean_vc||' 的分区不存在！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');
                     continue;
               else
                     
                     --判断唯一索引是否分区，若未分区，则clean；
                     execute immediate
                     'select count(1) from user_indexes t where t.table_name = :V_TBNAME' into v_index_partition_status using V_TBNAME;--判断是否建立索引（包括普通索引）
                     if v_index_partition_status = 0--无唯一索引，则直接truncate
                           then
                                v_index_partition_status := 'YES';
                     else
                                execute immediate'
                                select partitioned from
                                (
                                    select case when count(1)= 0 then ''YES'' else partitioned end as partitioned, rownum rn from
                                    (
                                        select partitioned, count(1) as partitioned_num
                                        from user_indexes t where t.table_name = :V_TBNAME
                                        --and (t.uniqueness = ''UNIQUE'')
                                        group by partitioned
                                    )b
                                    group by partitioned, rownum 
                                ) where rn =1'
                                into v_index_partition_status using V_TBNAME;
                     end if;
               end if;

               --至此，准备工作完成！！！
               ------------------------------------------------------------------------
               ------------------------------------------------------------------------
               if v_tm_grn = '2' and v_partitioned <> 'NO'
               --判断维度及分区是否存在
                    then
                        --execute immediate 'ALTER TABLE '||V_TBNAME||' NOLOGGING';                        
                        if v_index_partition_status = 'YES'
                           then
                                v_ssql := 'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''||V_TBNAME||''','''||v_tm_grn||''','||v_date_start_clean_vc||','''||v_date_start_clean_vc||''')';
                                --V_TBNAME,v_tm_grn,v_date_start_clean_vc,v_date_start_clean_vc
                                execute immediate v_ssql;
                        else
                                v_ssql := 'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX('''||V_TBNAME||''','''||v_tm_grn||''','||v_date_start_clean_vc||','''||v_date_start_clean_vc||''')';
                                --V_TBNAME,v_tm_grn,v_date_start_clean_vc,v_date_start_clean_vc
                                execute immediate v_ssql;
                        end if;

               elsif trunc(sysdate) = trunc(next_day(sysdate,'星期三'))-7 and v_tm_grn = '3' and v_partitioned <> 'NO'
                    --每周周三执行，清理X个月前的日期所在周的周级数据
                    then
                        --execute immediate 'ALTER TABLE '||V_TBNAME||' NOLOGGING';
                        if v_index_partition_status = 'YES'
                           then
                                v_ssql := 'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''||V_TBNAME||''','''||v_tm_grn||''','||v_date_start_clean_vc||','''||v_date_start_clean_vc||''')';
                                execute immediate v_ssql;
                        else
                                v_ssql := 'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX('''||V_TBNAME||''','''||v_tm_grn||''','||v_date_start_clean_vc||','''||v_date_start_clean_vc||''')';
                                execute immediate v_ssql;
                        end if;

               elsif trunc(sysdate) = trunc(last_day(sysdate)) and v_tm_grn = '4' and v_partitioned <> 'NO'
                    --每月末日执行，清理X个月前的日期所在月的月级数据
                    then
                        --execute immediate 'ALTER TABLE '||V_TBNAME||' NOLOGGING';
                        if v_index_partition_status = 'YES'
                           then
                                v_ssql := 'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''||V_TBNAME||''','''||v_tm_grn||''','||v_date_start_clean_vc||','''||v_date_start_clean_vc||''')';
                                execute immediate v_ssql;
                        else
                                v_ssql := 'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX('''||V_TBNAME||''','''||v_tm_grn||''','||v_date_start_clean_vc||','''||v_date_start_clean_vc||''')';
                               execute immediate v_ssql;
                        end if;

               elsif v_partitioned = 'YES' and v_partition_type = 'SHIN'
                    then
                        dbms_output.put_line('表 '||V_TBNAME||' 的清理触发时间未至！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');
               elsif v_partitioned = 'YES' and v_partition_type = 'SYS'  
                    then
                        dbms_output.put_line('表 '||V_TBNAME||' 下的SYS天级自动分区已中断！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');
                        continue;
               elsif v_partitioned = 'YES' and v_partition_type = 'NULL'  
                    then
                        dbms_output.put_line('表 '||V_TBNAME||' 下的SYS天级自动分区(指定分区)暂无数据，分区未生成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');
                        continue;
               end if;
            end if;
        end loop;
        commit;--******

        dbms_output.put_line('分区自动清理任务完成.');
    END PROC_PARTITION_CLEANUP_AUTO;
    
    
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE PROC_PARTITION_LOCATE(V_TABLE_NAME VARCHAR2, V_DATE_THRESHOLD_START VARCHAR2, V_PARTITION_NAME OUT VARCHAR2) IS
    v_high_value_vc                 varchar2(20);

    TYPE PARTITION_TYPE IS RECORD(
        table_name      varchar2(200),
        partition_name  varchar2(50),
        high_value      varchar2(100)
    );

    -- 定义基于记录的嵌套表
    TYPE NESTED_TYPE IS TABLE OF PARTITION_TYPE;
    -- 声明集合变量
    NESTED_TAB      NESTED_TYPE;
    -- 定义了一个变量来作为limit的值
    V_LIMIT         PLS_INTEGER := 500;
    -- 定义变量来记录FETCH次数
    V_COUNTER       INTEGER := 0;

    cursor CUR_PARTITION is --字典表内获取非标准分区的分区名
    select table_name,t.partition_name,t.high_value from USER_TAB_PARTITIONS t where table_name = V_TABLE_NAME and partition_name not like 'P2%'--SYS_21313
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历

    BEGIN
        OPEN CUR_PARTITION; --开始索引字典表
        LOOP--------------
            FETCH CUR_PARTITION BULK COLLECT INTO NESTED_TAB LIMIT V_LIMIT;
            EXIT WHEN NESTED_TAB.count = 0;
            V_COUNTER := V_COUNTER + 1; 
            FOR I IN NESTED_TAB.FIRST .. NESTED_TAB.LAST
            LOOP
                --区分TIMESTAMP类型：TIMESTAMP' 2019-04-25 00:00:00'
                if substr(NESTED_TAB(I).high_value, 1, 2) = 'TI'
                    then v_high_value_vc := substr(NESTED_TAB(I).high_value, 12, 10);
                else 
                    v_high_value_vc := substr(NESTED_TAB(I).high_value, 11, 10); --正常自增：less than 2019-07-14 ...
                end if;

                if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    V_PARTITION_NAME := NESTED_TAB(I).partition_name;
                    dbms_output.put_line('目标分区名：'||V_PARTITION_NAME);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
                end if;
            END LOOP;
        END LOOP;
        close cur_partition;--------------

        if V_PARTITION_NAME is NULL 
            then V_PARTITION_NAME := 'NULL'; 
        end if;

    END PROC_PARTITION_LOCATE;

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE SQL_GET_DDL_WK(I_FROM_TABLENAME VARCHAR2,  I_TM_GRN VARCHAR2,  I_FROM_OWNER VARCHAR2, I_TABLESPACE VARCHAR2, I_DATE_THRESHOLD_START VARCHAR2) is
     V_DATE  date;
     V_COB_TABLESQL CLOB;--元数据
     V_COB_TBSP_PARTITIONSQL CLOB;--表空间&分区存储
     --V_COB_A2WKA varchar2(500);
     V_SSQL varchar2(500);
     V_DATE_FIELD  varchar2(30);--时间字段名
     V_DATE_THRESHOLD_PART varchar2(20);
     V_PART_NAME   varchar2(30);--分区名拼接
     PK_FLAG number;
     --V_DATE_NAME   varchar2(200);--分区扩展时间段SQL拼接

     BEGIN
        --仅保留元数据，其余部分剔除
        BEGIN
          --关闭存储、表空间属性
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',FALSE);
          --DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',TRUE);

          --关闭创建表的PCTFREE、NOCOMPRESS等属性
          --PCTFREE：块保留10%的空间留给更新该块数据使用
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',FALSE);
          --分区信息
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PARTITIONING',FALSE);
          --输出信息采用缩排或换行格式化
          /*DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',FALSE);*/--编译权限问题，不开启
        END;
                
        SELECT DBMS_METADATA.GET_DDL('TABLE',I_FROM_TABLENAME,I_FROM_OWNER)
        INTO V_COB_TABLESQL
        FROM DUAL;--保存元数据
        --dbms_output.put_line('1：'||V_COB_TABLESQL);--打印分区表创建语句
        
        --主键约束是否存在判断
        select count(distinct u.constraint_type) into PK_FLAG
        from user_constraints u where u.table_name = I_FROM_TABLENAME--'NE_MME_L'
        and u.constraint_type = 'P';    
        
        --元数据表名拼接 'WK_'
        if PK_FLAG <> 0 --源表含约束时，去除约束生成条件
          then
            V_COB_TABLESQL := REPLACE(REPLACE(V_COB_TABLESQL, '"."', '"."WK_'),'CONSTRAINT','<');
            V_COB_TABLESQL := regexp_substr(V_COB_TABLESQL, '[^<]+',1,1,'i');--去除索引标记
            V_COB_TABLESQL := substr(V_COB_TABLESQL ,1 ,instr(V_COB_TABLESQL ,',' ,-1 ,1)-1)||'
       )';--截掉最后一个逗号，并添加右括号' )' 
        else--源表无约束时，正常拼接
            V_COB_TABLESQL := REPLACE(V_COB_TABLESQL, '"."', '"."WK_');  
        end if;
        
        --获取时间字段名！！！
        select column_name into V_DATE_FIELD from USER_TAB_COLUMNS t
        where column_id||'-'||table_name in
        (
            select min(t.column_id)||'-'||max(table_name) from USER_TAB_COLUMNS t
            where t.table_name = I_FROM_TABLENAME--'LC_INDEX_LXN_BAD_CELL_MTH_ORC_TEST'
            and t.data_type = 'DATE' --order by column_id
            --group by column_name
        );
        --dbms_output.put_line('2：'||V_COB_TABLESQL);--打印分区表创建语句
        
        --分区名对应时间戳
        V_DATE := trunc(to_date(I_DATE_THRESHOLD_START,'yyyymmdd'), 'mm');--起始时间戳'yyyymmdd'格式化--in：2019/07/02 out：2019/07/01

        if I_TM_GRN = '2'
          then
            --天级也从首日重新建立分区
            V_PART_NAME := 'P_' ||to_char(V_DATE,'yyyymmdd');--P_20190702
            V_DATE_THRESHOLD_PART := to_char(V_DATE + 1,'YYYY-MM-DD');--2019-07-03
            --V_DATE_NAME := ' VALUES LESS THAN (TO_DATE('''|| V_DATE_THRESHOLD_PART||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间
            V_COB_TBSP_PARTITIONSQL :=
            'PARTITION BY RANGE ('||V_DATE_FIELD||')
            (
              PARTITION '||V_PART_NAME||' VALUES LESS THAN (TO_DATE('''||V_DATE_THRESHOLD_PART||' 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN''))
                TABLESPACE '||I_TABLESPACE||'
            )'
            ;
            V_COB_TABLESQL := V_COB_TABLESQL ||V_COB_TBSP_PARTITIONSQL;

        elsif I_TM_GRN = '3'
          then
            V_DATE := next_day(V_DATE, '星期一') - 7;--起始时间戳'yyyymmdd'格式化--in：2019/07/02 out：2019/07/01
            --trunc(next_day(sysdate,'星期一'))-7
            --周级也从首日重新建立分区
            V_PART_NAME := 'P_' ||to_char(V_DATE,'yyyymmdd');--P_20190701
            V_DATE_THRESHOLD_PART := to_char(V_DATE + 7,'YYYY-MM-DD');--2019-07-08
            --V_DATE_NAME := ' VALUES LESS THAN (TO_DATE('''|| V_DATE_THRESHOLD_PART||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间
            V_COB_TBSP_PARTITIONSQL :=
            'PARTITION BY RANGE ('||V_DATE_FIELD||')
            (
              PARTITION '||V_PART_NAME||' VALUES LESS THAN (TO_DATE('''||V_DATE_THRESHOLD_PART||' 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN''))
                TABLESPACE '||I_TABLESPACE||'
            )'
            ;
            V_COB_TABLESQL := V_COB_TABLESQL ||V_COB_TBSP_PARTITIONSQL;

        elsif I_TM_GRN = '4'
          then
            V_PART_NAME := 'P_' ||to_char(V_DATE,'yyyymmdd');--P_20190701
            --V_DATE_THRESHOLD_PART := to_char(add_months(V_DATE,1),'YYYY-MM-DD');--2019-08-01
            --写法2：sysdate + interval '-1' MONTH
            V_DATE_THRESHOLD_PART := to_char((V_DATE + interval '1' MONTH),'YYYY-MM-DD');--2019-08-01
            --V_DATE_NAME := ' VALUES LESS THAN (TO_DATE('''||V_DATE_THRESHOLD_PART||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间

            V_COB_TBSP_PARTITIONSQL :=
            'PARTITION BY RANGE ('||V_DATE_FIELD||')
            (
              PARTITION '||V_PART_NAME||' VALUES LESS THAN (TO_DATE('''||V_DATE_THRESHOLD_PART||' 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN''))
                TABLESPACE '||I_TABLESPACE||'
            )'
            ;
            --dbms_output.put_line(V_COB_TABLESQL);--打印分区表创建语句
            V_COB_TABLESQL := V_COB_TABLESQL ||V_COB_TBSP_PARTITIONSQL;

        end if;
        --dbms_output.put_line('3：'||V_COB_TBSP_PARTITIONSQL);--打印分区表创建语句


        --留存数据方法2：新建分区表WK_A，数据回库，DROP原表A，重命名分区表WK_A至A
        --重建WK_A（含分区）A → DDL_SQL → WK_A
        --dbms_output.put_line('4：'||V_COB_TABLESQL);--打印分区表创建语句
        EXECUTE IMMEDIATE V_COB_TABLESQL;
        
        --增加新表 WK_A 的分区
        --PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_ADD_RANGE('WK_'||I_FROM_TABLENAME, I_TM_GRN ,I_DATE_THRESHOLD_START, TO_CHAR(SYSDATE,'yyyymmdd'),I_TABLESPACE);
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_ADD_RANGE('WK_'||I_FROM_TABLENAME, 
                                                                                                            I_TM_GRN, 
                                                                                                            to_char(V_DATE,'yyyymmdd'), 
                                                                                                            I_DATE_THRESHOLD_START, 
                                                                                                            I_TABLESPACE);    
        --数据回表 A → WK_A
        EXECUTE IMMEDIATE 'ALTER TABLE WK_'||I_FROM_TABLENAME||' NOLOGGING';             
        --V_COB_A2WKA := 'INSERT /*+append */ INTO WK_'||I_FROM_TABLENAME||' SELECT * FROM '||I_FROM_TABLENAME;
        --EXECUTE IMMEDIATE V_COB_A2WKA;
        
        --EXECUTE IMMEDIATE 'INSERT INTO WK_'||I_FROM_TABLENAME||' SELECT * FROM '||I_FROM_TABLENAME;
        --dbms_output.put_line('5：'||V_COB_A2WKA);--打印分区表创建语句
        COMMIT;
        --EXECUTE IMMEDIATE 'ALTER TABLE WK_'||I_FROM_TABLENAME||' LOGGING';             
        --dbms_output.put_line('表 WK_'||I_FROM_TABLENAME||' 数据重入完成！');--执行SQL后输出标记    
        
        --删除原表A
        --EXECUTE IMMEDIATE 'DROP TABLE '||I_FROM_TABLENAME||' PURGE'; 
        
        --重命名新表 WK_A（含分区） 至 A
        V_SSQL := 'ALTER TABLE WK_'||I_FROM_TABLENAME||' RENAME TO '||I_FROM_TABLENAME;
        dbms_output.put_line('6：'||V_SSQL);--打印分区表创建语句
        --EXECUTE IMMEDIATE V_SSQL;
        dbms_output.put_line('表 WK_'||I_FROM_TABLENAME||' 已重命名至 '||I_FROM_TABLENAME||' ！');--执行SQL后输出标记
        


     END SQL_GET_DDL_WK;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE SQL_GET_DDL(I_FROM_TABLENAME VARCHAR2,  I_TM_GRN VARCHAR2,  I_FROM_OWNER VARCHAR2, I_TABLESPACE VARCHAR2, I_DATE_THRESHOLD_START VARCHAR2) is
     V_DATE  date;
     V_COB_TABLESQL CLOB;--元数据
     V_COB_TBSP_PARTITIONSQL CLOB;--表空间&分区存储
     V_COB_A2WKA varchar2(500);
     V_SSQL varchar2(500);
     V_DATE_FIELD  varchar2(30);--时间字段名
     V_DATE_THRESHOLD_PART varchar2(20);
     V_PART_NAME   varchar2(30);--分区名拼接
     PK_FLAG number;
     --V_DATE_NAME   varchar2(200);--分区扩展时间段SQL拼接

     BEGIN
        --仅保留元数据，其余部分剔除
        BEGIN
          --关闭存储、表空间属性
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',FALSE);
          --DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',TRUE);

          --关闭创建表的PCTFREE、NOCOMPRESS等属性
          --PCTFREE：块保留10%的空间留给更新该块数据使用
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',FALSE);
          --分区信息
          DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PARTITIONING',FALSE);
          --输出信息采用缩排或换行格式化
          /*DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',FALSE);*/--编译权限问题，不开启
        END;
                
        SELECT DBMS_METADATA.GET_DDL('TABLE',I_FROM_TABLENAME,I_FROM_OWNER)
        INTO V_COB_TABLESQL
        FROM DUAL;--保存元数据
        --dbms_output.put_line('1：'||V_COB_TABLESQL);--打印分区表创建语句
        
        --主键约束是否存在判断
        select count(distinct u.constraint_type) into PK_FLAG
        from user_constraints u where u.table_name = I_FROM_TABLENAME--'NE_MME_L'
        and u.constraint_type = 'P';    
        
        --元数据表名拼接 'WK_'
        if PK_FLAG <> 0 --源表含约束时，去除约束生成条件
          then
            V_COB_TABLESQL := REPLACE(REPLACE(V_COB_TABLESQL, '"."', '"."WK_'),'CONSTRAINT','<');
            V_COB_TABLESQL := regexp_substr(V_COB_TABLESQL, '[^<]+',1,1,'i');--去除索引标记
            V_COB_TABLESQL := substr(V_COB_TABLESQL ,1 ,instr(V_COB_TABLESQL ,',' ,-1 ,1)-1)||'
       )';--截掉最后一个逗号，并添加右括号' )' 
        else--源表无约束时，正常拼接
            V_COB_TABLESQL := REPLACE(V_COB_TABLESQL, '"."', '"."WK_');  
        end if;
        
        --获取时间字段名！！！
        select column_name into V_DATE_FIELD from USER_TAB_COLUMNS t
        where column_id||'-'||table_name in
        (
            select min(t.column_id)||'-'||max(table_name) from USER_TAB_COLUMNS t
            where t.table_name = I_FROM_TABLENAME--'LC_INDEX_LXN_BAD_CELL_MTH_ORC_TEST'
            and t.data_type = 'DATE' --order by column_id
            --group by column_name
        );
        --dbms_output.put_line('2：'||V_COB_TABLESQL);--打印分区表创建语句
        
        --分区名对应时间戳
        V_DATE := trunc(to_date(I_DATE_THRESHOLD_START,'yyyymmdd'), 'mm');--起始时间戳'yyyymmdd'格式化--in：2019/07/02 out：2019/07/01

        if I_TM_GRN = '2'
          then
            --天级也从首日重新建立分区
            V_PART_NAME := 'P_' ||to_char(V_DATE,'yyyymmdd');--P_20190702
            V_DATE_THRESHOLD_PART := to_char(V_DATE + 1,'YYYY-MM-DD');--2019-07-03
            --V_DATE_NAME := ' VALUES LESS THAN (TO_DATE('''|| V_DATE_THRESHOLD_PART||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间
            V_COB_TBSP_PARTITIONSQL :=
            'PARTITION BY RANGE ('||V_DATE_FIELD||')
            (
              PARTITION '||V_PART_NAME||' VALUES LESS THAN (TO_DATE('''||V_DATE_THRESHOLD_PART||' 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN''))
                TABLESPACE '||I_TABLESPACE||'
            )'
            ;
            V_COB_TABLESQL := V_COB_TABLESQL ||V_COB_TBSP_PARTITIONSQL;

        elsif I_TM_GRN = '3'
          then
            V_DATE := next_day(V_DATE, '星期一') - 7;--起始时间戳'yyyymmdd'格式化--in：2019/07/02 out：2019/07/01
            --trunc(next_day(sysdate,'星期一'))-7
            --周级也从首日重新建立分区
            V_PART_NAME := 'P_' ||to_char(V_DATE,'yyyymmdd');--P_20190701
            V_DATE_THRESHOLD_PART := to_char(V_DATE + 7,'YYYY-MM-DD');--2019-07-08
            --V_DATE_NAME := ' VALUES LESS THAN (TO_DATE('''|| V_DATE_THRESHOLD_PART||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间
            V_COB_TBSP_PARTITIONSQL :=
            'PARTITION BY RANGE ('||V_DATE_FIELD||')
            (
              PARTITION '||V_PART_NAME||' VALUES LESS THAN (TO_DATE('''||V_DATE_THRESHOLD_PART||' 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN''))
                TABLESPACE '||I_TABLESPACE||'
            )'
            ;
            V_COB_TABLESQL := V_COB_TABLESQL ||V_COB_TBSP_PARTITIONSQL;

        elsif I_TM_GRN = '4'
          then
            V_PART_NAME := 'P_' ||to_char(V_DATE,'yyyymmdd');--P_20190701
            --V_DATE_THRESHOLD_PART := to_char(add_months(V_DATE,1),'YYYY-MM-DD');--2019-08-01
            --写法2：sysdate + interval '-1' MONTH
            V_DATE_THRESHOLD_PART := to_char((V_DATE + interval '1' MONTH),'YYYY-MM-DD');--2019-08-01
            --V_DATE_NAME := ' VALUES LESS THAN (TO_DATE('''||V_DATE_THRESHOLD_PART||' 00:00:00'',''YYYY-MM-DD HH24:MI:SS'')) ';--拼接时间

            V_COB_TBSP_PARTITIONSQL :=
            'PARTITION BY RANGE ('||V_DATE_FIELD||')
            (
              PARTITION '||V_PART_NAME||' VALUES LESS THAN (TO_DATE('''||V_DATE_THRESHOLD_PART||' 00:00:00'', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN''))
                TABLESPACE '||I_TABLESPACE||'
            )'
            ;
            --dbms_output.put_line(V_COB_TABLESQL);--打印分区表创建语句
            V_COB_TABLESQL := V_COB_TABLESQL ||V_COB_TBSP_PARTITIONSQL;

        end if;
        --dbms_output.put_line('3：'||V_COB_TBSP_PARTITIONSQL);--打印分区表创建语句


        --留存数据方法2：新建分区表WK_A，数据回库，DROP原表A，重命名分区表WK_A至A
        --重建WK_A（含分区）A → DDL_SQL → WK_A
        --dbms_output.put_line('4：'||V_COB_TABLESQL);--打印分区表创建语句
        EXECUTE IMMEDIATE V_COB_TABLESQL;
        
        --增加新表 WK_A 的分区
        --PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_ADD_RANGE('WK_'||I_FROM_TABLENAME, I_TM_GRN ,I_DATE_THRESHOLD_START, TO_CHAR(SYSDATE,'yyyymmdd'),I_TABLESPACE);
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_ADD_RANGE('WK_'||I_FROM_TABLENAME, 
                                                                                                            I_TM_GRN, 
                                                                                                            to_char(V_DATE,'yyyymmdd'), 
                                                                                                            I_DATE_THRESHOLD_START, 
                                                                                                            I_TABLESPACE);    
        --数据回表 A → WK_A
        EXECUTE IMMEDIATE 'ALTER TABLE WK_'||I_FROM_TABLENAME||' NOLOGGING';  
        V_COB_A2WKA := 'INSERT /*+append */ INTO WK_'||I_FROM_TABLENAME||' SELECT * FROM '||I_FROM_TABLENAME;
        --EXECUTE IMMEDIATE V_COB_A2WKA;
        
        --EXECUTE IMMEDIATE 'INSERT INTO WK_'||I_FROM_TABLENAME||' SELECT * FROM '||I_FROM_TABLENAME;
        dbms_output.put_line('数据回表语句（执行暂不打开）：'||V_COB_A2WKA);--打印分区表创建语句
        COMMIT;
        --dbms_output.put_line('表 WK_'||I_FROM_TABLENAME||' 数据重入完成！');--执行SQL后输出标记    
        
        --删除原表A
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.DROPTABLE_IFEXISTS(I_FROM_TABLENAME, '1');
        --EXECUTE IMMEDIATE 'DROP TABLE '||I_FROM_TABLENAME||' PURGE'; 
        
        --重命名新表 WK_A（含分区） 至 A
        V_SSQL := 'ALTER TABLE WK_'||I_FROM_TABLENAME||' RENAME TO '||I_FROM_TABLENAME;
        dbms_output.put_line('6：'||V_SSQL);--打印分区表创建语句
        EXECUTE IMMEDIATE V_SSQL;
        dbms_output.put_line('表 WK_'||I_FROM_TABLENAME||' 已重命名至 '||I_FROM_TABLENAME||' ！');--执行SQL后输出标记
        


     END SQL_GET_DDL;

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE DROPTABLE_IFEXISTS(I_TABLE_NAME VARCHAR2, PURGE_FLAG NUMBER) AS
    --drop table
    --purge_flag = 1 闪回删除 0 无闪回删除
    v_tablename varchar2(50);
    v_flag number(10 ,0) := 0;
    ssql varchar2(300);
    BEGIN
          v_tablename:=UPPER(I_TABLE_NAME);
          ssql:= 'select count(*) from USER_TABLES where table_name='''||v_tablename|| '''';
          execute immediate ssql into v_flag;
          if v_flag = 1 and purge_flag = 1 then
             begin
                ssql:= 'drop table '||v_tablename||' purge' ;
                execute immediate ssql;
                commit;
             end;
          elsif v_flag = 1 and purge_flag = 0 then
             begin
                ssql:= 'drop table '||v_tablename;
                execute immediate ssql;
                commit;
             end;
          end if ;
    END DROPTABLE_IFEXISTS;
    
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE PROC_LOGGING(I_SDATE DATE, I_PKG_NAME VARCHAR2,  I_INSIDE_LOOP_LOG NUMBER,  I_EXSIT_FLAG NUMBER) IS
    BEGIN
        insert into DB_CHECK(execute_sdate, execute_sql, v_loop_log, v_exsit_flag)
        (
          select I_SDATE,
          I_PKG_NAME||'('||to_char(I_SDATE, 'yyyymmdd')||')',
          I_INSIDE_LOOP_LOG,
          I_EXSIT_FLAG
          from dual
        );
        commit;
        dbms_output.put_line('LOG采集完成...');

    END PROC_LOGGING;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  PROCEDURE PROC_TEST IS
    BEGIN
        insert into LC_INDEX_LXN_BAD_CELL_DAY_ORC_TEST(start_time, area, area_level)
        (
          select sysdate,
          '上海',
          '上海'
          from dual
        );
        commit;
        dbms_output.put_line('LC_INDEX_LXN_BAD_CELL_DAY_ORC_TEST数据输入完成...');

    END PROC_TEST;
        

END PKG_MANAGE_SYSTEM_SHIN_PLUS8;
/

