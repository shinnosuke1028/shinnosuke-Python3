CREATE OR REPLACE PACKAGE BODY PKG_LC_OMC_NR_CELL AS
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  --OMC_NR_3
  --in：OMC_NR_1
  --out：OMC_NR_3
  --天表：每天启动，汇聚当日数据
  PROCEDURE PROC_OMC_NR_3(V_DATE_THRESHOLD_START VARCHAR2, V_DATE_HOUR VARCHAR2) IS
    V_TBNAME varchar2(50);
    V_PARTITION_NAME varchar2(30);
    v_date_start  date;
    v_date_end  date;
    v_inside_loop_log number := 0;
    v_insert_cnt   number;
    --v_insert_repeat   number;
    v_ssql varchar2(4000);
    v_clean_flag number;

    BEGIN
        --起止时间戳格式化拼接为小时级别
        if V_DATE_HOUR IS NOT NULL
        then
            v_date_start := to_date(V_DATE_THRESHOLD_START||' '||V_DATE_HOUR, 'YYYYMMDD HH24');--起始时间戳'yyyymmdd'格式化--2019/06/26
            v_date_end := to_date(V_DATE_THRESHOLD_START||' '||V_DATE_HOUR, 'YYYYMMDD HH24') + numtodsinterval(1,'hour');
        else--未指定，汇聚全天
            v_date_start := to_date(V_DATE_THRESHOLD_START||' 0', 'YYYYMMDD HH24');
            v_date_end := to_date(V_DATE_THRESHOLD_START||' 0', 'YYYYMMDD HH24') + numtodsinterval(1,'day');
        end if;
        
        --PLUS7暂未部署，PLUS8已部署
        --PKG_MANAGE_SYSTEM_PLUS7_0.PROC_PARTITION_CLEANUP_RANGE('LRNOP','OMC_NR_8',v_date_start,v_date_start,'0','0');
        
        --开始分区数据清理
        V_TBNAME := 'OMC_NR_3';

        --索引分区名
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);

        --清理（兼容小时/天）
        IF V_DATE_HOUR IS NULL
        THEN
            execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
            while v_clean_flag !=0 loop
                exit when v_inside_loop_log >=5; --超次退出
                select
                'CALL PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX('''|| TABLE_NAME||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                where s.table_name = V_TBNAME;
                execute immediate v_ssql;
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
                v_inside_loop_log := v_inside_loop_log +1;
            end loop;
        ELSE
            execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||') where s_hour='||V_DATE_HOUR into v_clean_flag;
            while v_clean_flag !=0 loop
                exit when v_inside_loop_log >=5; --超次退出
                v_ssql := 'DELETE FROM '|| V_TBNAME||' PARTITION('||v_partition_name||') WHERE S_HOUR='||V_DATE_HOUR;
                execute immediate v_ssql;
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||') where s_hour='||V_DATE_HOUR into v_clean_flag;
                v_inside_loop_log := v_inside_loop_log +1;
            end loop;
        END IF;
        --准备工作完成！！
        
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OMC_NR_1', V_DATE_THRESHOLD_START, V_PARTITION_NAME);
        --5G小时级数据插入
        --execute immediate
        
        v_ssql :=
        'merge /*+ parallel(a,2)  nologging */ into OMC_NR_3 a
        using
        (
            select 
            s_date, s_month, s_week, s_day, s_hour, to_date('||V_DATE_THRESHOLD_START||' ||'' ''||s_hour, ''yyyymmdd hh24'') as start_time,
            gnb_id, cell_id, tac, ecgi, vendor,
            avg(nr_0001) as nr_0001, sum(nr_0002) as nr_0002, sum(nr_0003) as nr_0003,
            sum(nr_0004) as nr_0004, sum(nr_0005) as nr_0005, sum(nr_0006) as nr_0006,
            sum(nr_0007) as nr_0007, sum(nr_0008) as nr_0008, sum(nr_0009) as nr_0009,
            sum(nr_0010) as nr_0010, sum(nr_0011) as nr_0011, sum(nr_0012) as nr_0012,
            sum(nr_0013) as nr_0013, avg(nr_0014) as nr_0014, avg(nr_0015) as nr_0015,
            avg(nr_0016) as nr_0016, avg(nr_0017) as nr_0017, sum(nr_0018) as nr_0018,
            sum(nr_0019) as nr_0019, sum(nr_0020) as nr_0020, sum(nr_0021) as nr_0021, 
            sum(nr_0022) as nr_0022, sum(nr_0023) as nr_0023, avg(nr_0024) as nr_0024,
            max(nr_0025) as nr_0025, sum(nr_0026) as nr_0026, sum(nr_0027) as nr_0027
            from OMC_NR_1 partition('||V_PARTITION_NAME||')
            where start_time >= :v_date_start and start_time < :v_date_end
            -- s_date = to_date(V_DATE_THRESHOLD_START, ''YYYYMMDD'')
            -- and s_hour = V_DATE_HOUR
            group by s_date,s_month,s_week,s_day,s_hour,gnb_id,cell_id,tac,ecgi,vendor
        )b 
        on (a.start_time = b.start_time and a.ecgi = b.ecgi)
        when matched then update set 
        a.s_date = b.s_date, a.s_month = b.s_month, a.s_week = b.s_week, a.s_day = b.s_day, a.s_hour = b.s_hour, 
        a.gnb_id = b.gnb_id, a.cell_id = b.cell_id, a.tac = b.tac, a.vendor = b.vendor, 
        a.nr_0001 = b.nr_0001, a.nr_0002 = b.nr_0002, a.nr_0003 = b.nr_0003, a.nr_0004 = b.nr_0004, a.nr_0005 = b.nr_0005, 
        a.nr_0006 = b.nr_0006, a.nr_0007 = b.nr_0007, a.nr_0008 = b.nr_0008, a.nr_0009 = b.nr_0009, a.nr_0010 = b.nr_0010, 
        a.nr_0011 = b.nr_0011, a.nr_0012 = b.nr_0012, a.nr_0013 = b.nr_0013, a.nr_0014 = b.nr_0014, a.nr_0015 = b.nr_0015, 
        a.nr_0016 = b.nr_0016, a.nr_0017 = b.nr_0017, a.nr_0018 = b.nr_0018, a.nr_0019 = b.nr_0019, a.nr_0020 = b.nr_0020, 
        a.nr_0021 = b.nr_0021, a.nr_0022 = b.nr_0022, a.nr_0023 = b.nr_0023, a.nr_0024 = b.nr_0024, a.nr_0025 = b.nr_0025, 
        a.nr_0026 = b.nr_0026, a.nr_0027 = b.nr_0027
        when not matched then insert
        (
            a.s_date, a.s_month, a.s_week, a.s_day, a.s_hour, a.start_time, 
            a.gnb_id, a.cell_id, a.tac, a.ecgi, a.vendor, 
            a.nr_0001, a.nr_0002, a.nr_0003, a.nr_0004, a.nr_0005, a.nr_0006, a.nr_0007, a.nr_0008, a.nr_0009, a.nr_0010, 
            a.nr_0011, a.nr_0012, a.nr_0013, a.nr_0014, a.nr_0015, a.nr_0016, a.nr_0017, a.nr_0018, a.nr_0019, a.nr_0020, 
            a.nr_0021, a.nr_0022, a.nr_0023, a.nr_0024, a.nr_0025, a.nr_0026, a.nr_0027
        )
        values
        (
            b.s_date, b.s_month, b.s_week, b.s_day, b.s_hour, b.start_time, 
            b.gnb_id, b.cell_id, b.tac, b.ecgi, b.vendor, 
            b.nr_0001, b.nr_0002, b.nr_0003, b.nr_0004, b.nr_0005, b.nr_0006, b.nr_0007, b.nr_0008, b.nr_0009, b.nr_0010, 
            b.nr_0011, b.nr_0012, b.nr_0013, b.nr_0014, b.nr_0015, b.nr_0016, b.nr_0017, b.nr_0018, b.nr_0019, b.nr_0020, 
            b.nr_0021, b.nr_0022, b.nr_0023, b.nr_0024, b.nr_0025, b.nr_0026, b.nr_0027
        )' /*using v_date_start, v_date_end*/;
        --commit;
        --dbms_output.put_line(v_ssql);        
        execute immediate v_ssql using v_date_start, v_date_end;
        commit;
        
        
        --入库数量判断
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_insert_cnt;

            
        --重复率判断
        --正常情况下，按照CELL_NAME唯一
        /*select count(1) into v_insert_repeat from
        (
               select count(1) from OMC_NR_8 t 
               where t.start_time  = v_date_start \*and t.s_date< v_date_end*\ 
               group by t.cell_name\*, t.enb_name, t.ci*\ having count(1)>1
        );*/
        dbms_output.put_line('表 OMC_NR_3 小时级数据插入完成！时间戳：'||to_char(v_date_start,'yyyymmdd hh24')||'，入库数据行数：'||v_insert_cnt||'.
        ');
        
    END PROC_OMC_NR_3;
    
    
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  --OMC_NR_8
  --in：OMC_NR_3
  --out：OMC_NR_8
  --天表：每天启动，汇聚当日数据
  PROCEDURE PROC_OMC_NR_8(V_DATE_THRESHOLD_START VARCHAR2) IS
    V_TBNAME varchar2(50);
    V_PARTITION_NAME varchar2(30);
    v_date_start  date;
    --v_date_end  date;
    v_inside_loop_log number := 0;
    v_insert_cnt   number;
    --v_insert_repeat   number;
    v_ssql varchar2(4000);
    v_clean_flag number;

    BEGIN
        --起止时间戳格式化拼接为小时级别
        /*if V_DATE_HOUR IS NOT NULL
        then
            v_date_start := to_date(V_DATE_THRESHOLD_START||' '||V_DATE_HOUR, 'YYYYMMDD HH24');--起始时间戳'yyyymmdd'格式化--2019/06/26
            v_date_end := to_date(V_DATE_THRESHOLD_START||' '||V_DATE_HOUR, 'YYYYMMDD HH24') + numtodsinterval(1,'hour');
        else--未指定，汇聚全天
            v_date_start := to_date(V_DATE_THRESHOLD_START||' 0', 'YYYYMMDD HH24');
            v_date_end := to_date(V_DATE_THRESHOLD_START||' 0', 'YYYYMMDD HH24') + numtodsinterval(1,'day');
        end if;*/
        
        --PLUS7暂未部署，PLUS8已部署
        --PKG_MANAGE_SYSTEM_PLUS7_0.PROC_PARTITION_CLEANUP_RANGE('LRNOP','OMC_NR_8',v_date_start,v_date_start,'0','0');
        
        --开始分区数据清理
        V_TBNAME := 'OMC_NR_8';

        --索引分区名
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);

        --清理
        if V_PARTITION_NAME <> 'NULL'
        then            
            execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
            while v_clean_flag !=0 loop
                exit when v_inside_loop_log >=5; --超次退出
                select
                'CALL PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX('''|| TABLE_NAME||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                where s.table_name = V_TBNAME;
                execute immediate v_ssql;
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
                v_inside_loop_log := v_inside_loop_log +1;
            end loop;
        END IF;
        --准备工作完成！！
        
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OMC_NR_3', V_DATE_THRESHOLD_START, V_PARTITION_NAME);
        --5G小时级数据插入
        --execute immediate
        
        v_ssql :=
        'merge /*+ parallel(a,2)  nologging */ into OMC_NR_8 a
        using
        (
            select 
            s_date, s_month, s_week, s_day, 0 s_hour, to_date('||V_DATE_THRESHOLD_START||', ''yyyymmdd hh24'') as start_time,
            gnb_id, cell_id, tac, ecgi, vendor,
            avg(nr_0001) as nr_0001, sum(nr_0002) as nr_0002, sum(nr_0003) as nr_0003,
            sum(nr_0004) as nr_0004, sum(nr_0005) as nr_0005, sum(nr_0006) as nr_0006,
            sum(nr_0007) as nr_0007, sum(nr_0008) as nr_0008, sum(nr_0009) as nr_0009,
            sum(nr_0010) as nr_0010, sum(nr_0011) as nr_0011, sum(nr_0012) as nr_0012,
            sum(nr_0013) as nr_0013, avg(nr_0014) as nr_0014, avg(nr_0015) as nr_0015,
            avg(nr_0016) as nr_0016, avg(nr_0017) as nr_0017, sum(nr_0018) as nr_0018,
            sum(nr_0019) as nr_0019, sum(nr_0020) as nr_0020, sum(nr_0021) as nr_0021, 
            sum(nr_0022) as nr_0022, sum(nr_0023) as nr_0023, avg(nr_0024) as nr_0024,
            max(nr_0025) as nr_0025, sum(nr_0026) as nr_0026, sum(nr_0027) as nr_0027
            from OMC_NR_3 partition('||V_PARTITION_NAME||')
            -- where start_time >= :v_date_start and start_time < :v_date_end
            -- s_date = to_date(V_DATE_THRESHOLD_START, ''YYYYMMDD'')
            -- and s_hour = V_DATE_HOUR
            group by s_date, s_month, s_week, s_day, gnb_id, cell_id, tac, ecgi, vendor
        )b 
        on (a.start_time = b.start_time and a.ecgi = b.ecgi)
        when matched then update set 
        a.s_date = b.s_date, a.s_month = b.s_month, a.s_week = b.s_week, a.s_day = b.s_day, a.s_hour = b.s_hour, 
        a.gnb_id = b.gnb_id, a.cell_id = b.cell_id, a.tac = b.tac, a.vendor = b.vendor, 
        a.nr_0001 = b.nr_0001, a.nr_0002 = b.nr_0002, a.nr_0003 = b.nr_0003, a.nr_0004 = b.nr_0004, a.nr_0005 = b.nr_0005, 
        a.nr_0006 = b.nr_0006, a.nr_0007 = b.nr_0007, a.nr_0008 = b.nr_0008, a.nr_0009 = b.nr_0009, a.nr_0010 = b.nr_0010, 
        a.nr_0011 = b.nr_0011, a.nr_0012 = b.nr_0012, a.nr_0013 = b.nr_0013, a.nr_0014 = b.nr_0014, a.nr_0015 = b.nr_0015, 
        a.nr_0016 = b.nr_0016, a.nr_0017 = b.nr_0017, a.nr_0018 = b.nr_0018, a.nr_0019 = b.nr_0019, a.nr_0020 = b.nr_0020, 
        a.nr_0021 = b.nr_0021, a.nr_0022 = b.nr_0022, a.nr_0023 = b.nr_0023, a.nr_0024 = b.nr_0024, a.nr_0025 = b.nr_0025, 
        a.nr_0026 = b.nr_0026, a.nr_0027 = b.nr_0027
        when not matched then insert
        (
            a.s_date, a.s_month, a.s_week, a.s_day, a.s_hour, a.start_time, 
            a.gnb_id, a.cell_id, a.tac, a.ecgi, a.vendor, 
            a.nr_0001, a.nr_0002, a.nr_0003, a.nr_0004, a.nr_0005, a.nr_0006, a.nr_0007, a.nr_0008, a.nr_0009, a.nr_0010, 
            a.nr_0011, a.nr_0012, a.nr_0013, a.nr_0014, a.nr_0015, a.nr_0016, a.nr_0017, a.nr_0018, a.nr_0019, a.nr_0020, 
            a.nr_0021, a.nr_0022, a.nr_0023, a.nr_0024, a.nr_0025, a.nr_0026, a.nr_0027
        )
        values
        (
            b.s_date, b.s_month, b.s_week, b.s_day, b.s_hour, b.start_time, 
            b.gnb_id, b.cell_id, b.tac, b.ecgi, b.vendor, 
            b.nr_0001, b.nr_0002, b.nr_0003, b.nr_0004, b.nr_0005, b.nr_0006, b.nr_0007, b.nr_0008, b.nr_0009, b.nr_0010, 
            b.nr_0011, b.nr_0012, b.nr_0013, b.nr_0014, b.nr_0015, b.nr_0016, b.nr_0017, b.nr_0018, b.nr_0019, b.nr_0020, 
            b.nr_0021, b.nr_0022, b.nr_0023, b.nr_0024, b.nr_0025, b.nr_0026, b.nr_0027
        )' /*using v_date_start, v_date_end*/;
        --commit;
        --dbms_output.put_line(v_ssql);        
        execute immediate v_ssql;-- using v_date_start, v_date_end;
        commit;
        
        
        --入库数量判断
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_insert_cnt;

            
        --重复率判断
        --正常情况下，按照CELL_NAME唯一
        /*select count(1) into v_insert_repeat from
        (
               select count(1) from OMC_NR_8 t 
               where t.start_time  = v_date_start \*and t.s_date< v_date_end*\ 
               group by t.cell_name\*, t.enb_name, t.ci*\ having count(1)>1
        );*/
        dbms_output.put_line('表 OMC_NR_8 天级数据插入完成！时间戳：'||to_char(v_date_start,'yyyymmdd hh24')||'，入库数据行数：'||v_insert_cnt||'.
        ');
        
    END PROC_OMC_NR_8;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  --in：OMC_NR_8
  --out：OMC_NR_9
  --周表：每周周一启动，汇聚上周数据
  PROCEDURE PROC_OMC_NR_9(V_DATE_THRESHOLD_START VARCHAR2) IS
    V_TBNAME varchar2(50);
    V_PARTITION_NAME varchar2(30);    
    v_date_start  date;
    v_date_end   date;
    V_DATE_START_VC varchar2(10);
    v_inside_loop_log number := 0;
    v_insert_cnt   number;
    v_insert_repeat   number;
    --v_ssql varchar2(300);
    v_ssql_t1 clob;
    v_ssql_t2 clob;
    v_ssql_t3 clob;
    v_clean_flag number;
    /*v_partition_name_1   varchar2(15);--当前日期0天后的分区名
    v_partition_name_2   varchar2(15);--当前日期1天后的分区名
    v_partition_name_3   varchar2(15);--当前日期2天后的分区名
    v_partition_name_4   varchar2(15);--当前日期3天后的分区名
    v_partition_name_5   varchar2(15);--当前日期4天后的分区名
    v_partition_name_6   varchar2(15);--当前日期5天后的分区名
    v_partition_name_7   varchar2(15);--当前日期6天后的分区名*/



    BEGIN
        V_TBNAME := 'OMC_NR_9';
        --返回输入时间戳所在周上周周一的日期，in：20191118~20191124  out：2019/11/11
        v_date_start := (trunc(next_day(to_date(v_date_threshold_start,'yyyymmdd'), '星期一'))-14);--2019/11/11
        v_date_end := (trunc(next_day(to_date(v_date_threshold_start,'yyyymmdd'), '星期一'))-14) + 6;--2019/11/17
        V_DATE_START_VC := to_char(v_date_start, 'yyyymmdd');--20191111
                    
        --PLUS7暂未部署
        --PKG_MANAGE_SYSTEM_PLUS7_0.PROC_PARTITION_CLEANUP_RANGE('LRNOP','OMC_NR_9',v_date_start,v_date_start,'0','0');
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_START_VC, V_PARTITION_NAME);

        --清理
        if V_PARTITION_NAME <> 'NULL'
        then            
            --分区数据清理
            execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
            while v_clean_flag != 0 loop
            exit when v_inside_loop_log >=5; --超次退出
                PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX(V_TBNAME, '3', V_DATE_START_VC, V_DATE_START_VC);
                /*select
                'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX('''|| table_name||''','''|| tm_grn ||''','''||to_char(v_date_start, 'yyyymmdd')||''','''||to_char(v_date_start, 'yyyymmdd')||''')'
                into v_ssql
                from FAST_DATA_PROCESS_TEMPLATE s
                where s.table_name = 'OMC_NR_9';
                execute immediate v_ssql;*/
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
                v_inside_loop_log := v_inside_loop_log +1;
            end loop;
        end if;
        
        --循环拼接分区表，遍历多日数据
        while v_date_start <= v_date_end loop
            --开启检索，进行分区名拼接
            PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OMC_NR_8', to_char(v_date_start, 'yyyymmdd'), V_PARTITION_NAME);
            if V_PARTITION_NAME <> 'NULL' 
                then
                --v_partition_name :='P_'||to_char(v_date_start, 'yyyymmdd');
                if v_date_start <> v_date_end 
                  then 
                    v_ssql_t1 := v_ssql_t1 || 'select * from OMC_NR_8 PARTITION('|| V_PARTITION_NAME||')
                    union all ';
                else 
                    v_ssql_t1 := v_ssql_t1 ||  'select * from OMC_NR_8 PARTITION('|| V_PARTITION_NAME||')';
                end if;
                v_date_start := v_date_start + 1;
            else
                v_date_start := v_date_start + 1;
                continue;
            end if;
        end loop; 
        --dbms_output.put_line(v_ssql_t1);
        
        --5G周级数据插入
        v_ssql_t2 := 
        /*'insert into OMC_NR_9
        select to_date('||V_DATE_START_VC||', ''yyyymmdd'') as start_time, --2019/06/17
        enb_name, ci, cell_name, --5G小区信息
        avg(n5_0001) n5_0001, --用户下行平均吞吐率(吉比特/秒) 
        avg(n5_0002) n5_0002, --用户上行平均吞吐率(吉比特/秒) 
        avg(n5_0003) n5_0003, --小区下行平均吞吐率(吉比特/秒) 
        avg(n5_0004) n5_0004, --小区上行平均吞吐率(吉比特/秒) 
        avg(n5_0005) n5_0005, --下行RB利用率(%) 
        avg(n5_0006) n5_0006, --上行RB利用率(%) 
        sum(n5_0007) n5_0007, --下行业务数据量(千比特) 
        sum(n5_0008) n5_0008, --上行业务数据量(千比特) 
        avg(n5_0009) n5_0009  --平均用户数时间
        from(';*/
        'merge /*+ parallel(a,2)  nologging */ into OMC_NR_9 a
        using
        (
            select 
            to_date('||V_DATE_START_VC||', ''yyyymmdd hh24'') s_date, min(s_month) s_month, min(s_week) s_week, min(s_day) s_day, 0 s_hour, to_date('||V_DATE_START_VC||', ''yyyymmdd hh24'') as start_time,
            gnb_id, cell_id, min(tac) tac, ecgi, vendor,
            avg(nr_0001) as nr_0001, sum(nr_0002) as nr_0002, sum(nr_0003) as nr_0003,
            sum(nr_0004) as nr_0004, sum(nr_0005) as nr_0005, sum(nr_0006) as nr_0006,
            sum(nr_0007) as nr_0007, sum(nr_0008) as nr_0008, sum(nr_0009) as nr_0009,
            sum(nr_0010) as nr_0010, sum(nr_0011) as nr_0011, sum(nr_0012) as nr_0012,
            sum(nr_0013) as nr_0013, avg(nr_0014) as nr_0014, avg(nr_0015) as nr_0015,
            avg(nr_0016) as nr_0016, avg(nr_0017) as nr_0017, sum(nr_0018) as nr_0018,
            sum(nr_0019) as nr_0019, sum(nr_0020) as nr_0020, sum(nr_0021) as nr_0021, 
            sum(nr_0022) as nr_0022, sum(nr_0023) as nr_0023, avg(nr_0024) as nr_0024,
            max(nr_0025) as nr_0025, sum(nr_0026) as nr_0026, sum(nr_0027) as nr_0027
            from(';
            
        v_ssql_t3 := 
        ')
            group by gnb_id, cell_id, ecgi, vendor
        )b 
        on (a.start_time = b.start_time and a.ecgi = b.ecgi)
        when matched then update set 
        a.s_date = b.s_date, a.s_month = b.s_month, a.s_week = b.s_week, a.s_day = b.s_day, a.s_hour = b.s_hour, 
        a.gnb_id = b.gnb_id, a.cell_id = b.cell_id, a.tac = b.tac, a.vendor = b.vendor, 
        a.nr_0001 = b.nr_0001, a.nr_0002 = b.nr_0002, a.nr_0003 = b.nr_0003, a.nr_0004 = b.nr_0004, a.nr_0005 = b.nr_0005, 
        a.nr_0006 = b.nr_0006, a.nr_0007 = b.nr_0007, a.nr_0008 = b.nr_0008, a.nr_0009 = b.nr_0009, a.nr_0010 = b.nr_0010, 
        a.nr_0011 = b.nr_0011, a.nr_0012 = b.nr_0012, a.nr_0013 = b.nr_0013, a.nr_0014 = b.nr_0014, a.nr_0015 = b.nr_0015, 
        a.nr_0016 = b.nr_0016, a.nr_0017 = b.nr_0017, a.nr_0018 = b.nr_0018, a.nr_0019 = b.nr_0019, a.nr_0020 = b.nr_0020, 
        a.nr_0021 = b.nr_0021, a.nr_0022 = b.nr_0022, a.nr_0023 = b.nr_0023, a.nr_0024 = b.nr_0024, a.nr_0025 = b.nr_0025, 
        a.nr_0026 = b.nr_0026, a.nr_0027 = b.nr_0027
        when not matched then insert
        (
            a.s_date, a.s_month, a.s_week, a.s_day, a.s_hour, a.start_time, 
            a.gnb_id, a.cell_id, a.tac, a.ecgi, a.vendor, 
            a.nr_0001, a.nr_0002, a.nr_0003, a.nr_0004, a.nr_0005, a.nr_0006, a.nr_0007, a.nr_0008, a.nr_0009, a.nr_0010, 
            a.nr_0011, a.nr_0012, a.nr_0013, a.nr_0014, a.nr_0015, a.nr_0016, a.nr_0017, a.nr_0018, a.nr_0019, a.nr_0020, 
            a.nr_0021, a.nr_0022, a.nr_0023, a.nr_0024, a.nr_0025, a.nr_0026, a.nr_0027
        )
        values
        (
            b.s_date, b.s_month, b.s_week, b.s_day, b.s_hour, b.start_time, 
            b.gnb_id, b.cell_id, b.tac, b.ecgi, b.vendor, 
            b.nr_0001, b.nr_0002, b.nr_0003, b.nr_0004, b.nr_0005, b.nr_0006, b.nr_0007, b.nr_0008, b.nr_0009, b.nr_0010, 
            b.nr_0011, b.nr_0012, b.nr_0013, b.nr_0014, b.nr_0015, b.nr_0016, b.nr_0017, b.nr_0018, b.nr_0019, b.nr_0020, 
            b.nr_0021, b.nr_0022, b.nr_0023, b.nr_0024, b.nr_0025, b.nr_0026, b.nr_0027
        )';
        
        v_ssql_t2 := v_ssql_t2||v_ssql_t1||v_ssql_t3;
        --dbms_output.put_line(v_ssql_t2);--打印拼接结果
        execute immediate v_ssql_t2;
        
        /*execute immediate 
        'insert into OMC_NR_9
        select to_date('||v_date_start_vc||', ''yyyymmdd'') as start_time, --2019/06/17
        enb_name, ci, cell_name, --5G小区信息
        avg(n5_0001) n5_0001, --用户下行平均吞吐率(吉比特/秒) 
        avg(n5_0002) n5_0002, --用户上行平均吞吐率(吉比特/秒) 
        avg(n5_0003) n5_0003, --小区下行平均吞吐率(吉比特/秒) 
        avg(n5_0004) n5_0004, --小区上行平均吞吐率(吉比特/秒) 
        avg(n5_0005) n5_0005, --下行RB利用率(%) 
        avg(n5_0006) n5_0006, --上行RB利用率(%) 
        sum(n5_0007) n5_0007, --下行业务数据量(千比特) 
        sum(n5_0008) n5_0008, --上行业务数据量(千比特) 
        avg(n5_0009) n5_0009  --平均用户数时间 
        from --OMC_NR_8 t where trunc(t.start_time,''dd'') between v_date_start and v_date_start+6--2019/06/17 ~ 2019/06/23
        (
            --分区模式                       
             select * from OMC_NR_8 PARTITION('||v_partition_name_1||')
             union all
             select * from OMC_NR_8 PARTITION('||v_partition_name_2||')
             union all
             select * from OMC_NR_8 PARTITION('||v_partition_name_3||')
             union all
             select * from OMC_NR_8 PARTITION('||v_partition_name_4||')
             union all
             select * from OMC_NR_8 PARTITION('||v_partition_name_5||')
             union all
             select * from OMC_NR_8 PARTITION('||v_partition_name_6||')
             union all
             select * from OMC_NR_8 PARTITION('||v_partition_name_7||')
        )T1--当前日期所在一周的数据拼接
        group by enb_name, ci, cell_name';
        execute immediate v_ssql;*/
        commit;
        
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_START_VC, V_PARTITION_NAME);
        --入库数量判断
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||') 
        group by s_date, ecgi having count(1)>1)'into v_insert_repeat;
        
        --正常情况下，按照CELL_NAME唯一
        dbms_output.put_line('表 OMC_NR_9 周级数据插入完成！时间戳：'||V_DATE_START_VC||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');
        
    END PROC_OMC_NR_9;




-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  --in：OMC_NR_8
  --out：OMC_NR_A
  --月表：每月首日启动，汇聚上月数据
  PROCEDURE PROC_OMC_NR_A(V_DATE_THRESHOLD_START VARCHAR2) IS
    V_TBNAME varchar2(50);
    V_PARTITION_NAME varchar2(30);   
    V_PARTITION_NAME_END varchar2(30);    
    v_date_start  date;
    v_date_end   date;
    V_DATE_START_VC varchar2(10);
    end_properly number := 0;
    --end_improperly number;
    
    v_inside_loop_log number := 0;
    v_insert_cnt   number;
    v_insert_repeat   number;
    --v_ssql varchar2(300);
    v_ssql_t1 clob;
    v_ssql_t2 clob;
    v_ssql_t3 clob;
    v_clean_flag number;
    BEGIN
        --返回输入时间戳所在周周一的日期，in：20191101~20191130  out：2019/11/01
        V_TBNAME := 'OMC_NR_A';
        v_date_start := trunc(to_date(v_date_threshold_start, 'yyyymmdd'), 'mm'); --2019/11/01
        v_date_end := trunc(last_day(v_date_start));--2019/11/30        

        v_date_start_vc := to_char(v_date_start, 'yyyymmdd'); --20191101
        
        --PLUS7暂未部署
        --PKG_MANAGE_SYSTEM_PLUS7_0.PROC_PARTITION_CLEANUP_RANGE('LRNOP','OMC_NR_A',v_date_start,v_date_start,'0','0');

        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_START_VC, V_PARTITION_NAME);

        --清理
        if V_PARTITION_NAME <> 'NULL'
        then            
            --分区数据清理
            execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
            while v_clean_flag != 0 loop
            exit when v_inside_loop_log >=5; --超次退出
                PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX(V_TBNAME, '4', V_DATE_START_VC, V_DATE_START_VC);
                /*select
                'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX('''|| table_name||''','''|| tm_grn ||''','''||to_char(v_date_start, 'yyyymmdd')||''','''||to_char(v_date_start, 'yyyymmdd')||''')'
                into v_ssql
                from FAST_DATA_PROCESS_TEMPLATE s
                where s.table_name = 'OMC_NR_9';
                execute immediate v_ssql;*/
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
                v_inside_loop_log := v_inside_loop_log +1;
            end loop;
        end if;
        
        --循环拼接分区表，遍历多日数据
        while v_date_start <= v_date_end loop
            --开启检索，进行分区名拼接
            PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OMC_NR_8', to_char(v_date_start, 'yyyymmdd'), V_PARTITION_NAME);
            if V_PARTITION_NAME <> 'NULL' 
                then
                if v_date_start <> v_date_end 
                  then 
                    v_ssql_t1 := v_ssql_t1 || 'select * from OMC_NR_8 PARTITION('|| V_PARTITION_NAME||')
                    union all ';
                    V_PARTITION_NAME_END := V_PARTITION_NAME; --存放一个分区存在的分区名
                else 
                    v_ssql_t1 := v_ssql_t1 || 'select * from OMC_NR_8 PARTITION('|| V_PARTITION_NAME||')';
                    end_properly := 1;
                end if;
                v_date_start := v_date_start + 1;
            else
                v_date_start := v_date_start + 1;
                continue;
            end if;
        end loop; 
        --下面这步是防止分区不存在，导致拼接结果最后多一个 union all，故在结尾拼接一个空的分区数据
        if end_properly = 1 
            then
                v_ssql_t1 := v_ssql_t1;
        else
            v_ssql_t1 := v_ssql_t1 || 'select * from OMC_NR_8 PARTITION('|| V_PARTITION_NAME_END||') where 1=2';
        end if;
        --dbms_output.put_line(v_ssql_t1);
        
        --5G周级数据插入
        v_ssql_t2 := 
        'merge /*+ parallel(a,2)  nologging */ into OMC_NR_A a
        using
        (
            select 
            to_date('||V_DATE_START_VC||', ''yyyymmdd hh24'') s_date, min(s_month) s_month, min(s_week) s_week, min(s_day) s_day, 0 s_hour, to_date('||V_DATE_START_VC||', ''yyyymmdd hh24'') as start_time,
            gnb_id, cell_id, min(tac) tac, ecgi, vendor,
            avg(nr_0001) as nr_0001, sum(nr_0002) as nr_0002, sum(nr_0003) as nr_0003,
            sum(nr_0004) as nr_0004, sum(nr_0005) as nr_0005, sum(nr_0006) as nr_0006,
            sum(nr_0007) as nr_0007, sum(nr_0008) as nr_0008, sum(nr_0009) as nr_0009,
            sum(nr_0010) as nr_0010, sum(nr_0011) as nr_0011, sum(nr_0012) as nr_0012,
            sum(nr_0013) as nr_0013, avg(nr_0014) as nr_0014, avg(nr_0015) as nr_0015,
            avg(nr_0016) as nr_0016, avg(nr_0017) as nr_0017, sum(nr_0018) as nr_0018,
            sum(nr_0019) as nr_0019, sum(nr_0020) as nr_0020, sum(nr_0021) as nr_0021, 
            sum(nr_0022) as nr_0022, sum(nr_0023) as nr_0023, avg(nr_0024) as nr_0024,
            max(nr_0025) as nr_0025, sum(nr_0026) as nr_0026, sum(nr_0027) as nr_0027
            from(';
            
        v_ssql_t3 := 
        ')
            group by gnb_id, cell_id, ecgi, vendor
        )b 
        on (a.start_time = b.start_time and a.ecgi = b.ecgi)
        when matched then update set 
        a.s_date = b.s_date, a.s_month = b.s_month, a.s_week = b.s_week, a.s_day = b.s_day, a.s_hour = b.s_hour, 
        a.gnb_id = b.gnb_id, a.cell_id = b.cell_id, a.tac = b.tac, a.vendor = b.vendor, 
        a.nr_0001 = b.nr_0001, a.nr_0002 = b.nr_0002, a.nr_0003 = b.nr_0003, a.nr_0004 = b.nr_0004, a.nr_0005 = b.nr_0005, 
        a.nr_0006 = b.nr_0006, a.nr_0007 = b.nr_0007, a.nr_0008 = b.nr_0008, a.nr_0009 = b.nr_0009, a.nr_0010 = b.nr_0010, 
        a.nr_0011 = b.nr_0011, a.nr_0012 = b.nr_0012, a.nr_0013 = b.nr_0013, a.nr_0014 = b.nr_0014, a.nr_0015 = b.nr_0015, 
        a.nr_0016 = b.nr_0016, a.nr_0017 = b.nr_0017, a.nr_0018 = b.nr_0018, a.nr_0019 = b.nr_0019, a.nr_0020 = b.nr_0020, 
        a.nr_0021 = b.nr_0021, a.nr_0022 = b.nr_0022, a.nr_0023 = b.nr_0023, a.nr_0024 = b.nr_0024, a.nr_0025 = b.nr_0025, 
        a.nr_0026 = b.nr_0026, a.nr_0027 = b.nr_0027
        when not matched then insert
        (
            a.s_date, a.s_month, a.s_week, a.s_day, a.s_hour, a.start_time, 
            a.gnb_id, a.cell_id, a.tac, a.ecgi, a.vendor, 
            a.nr_0001, a.nr_0002, a.nr_0003, a.nr_0004, a.nr_0005, a.nr_0006, a.nr_0007, a.nr_0008, a.nr_0009, a.nr_0010, 
            a.nr_0011, a.nr_0012, a.nr_0013, a.nr_0014, a.nr_0015, a.nr_0016, a.nr_0017, a.nr_0018, a.nr_0019, a.nr_0020, 
            a.nr_0021, a.nr_0022, a.nr_0023, a.nr_0024, a.nr_0025, a.nr_0026, a.nr_0027
        )
        values
        (
            b.s_date, b.s_month, b.s_week, b.s_day, b.s_hour, b.start_time, 
            b.gnb_id, b.cell_id, b.tac, b.ecgi, b.vendor, 
            b.nr_0001, b.nr_0002, b.nr_0003, b.nr_0004, b.nr_0005, b.nr_0006, b.nr_0007, b.nr_0008, b.nr_0009, b.nr_0010, 
            b.nr_0011, b.nr_0012, b.nr_0013, b.nr_0014, b.nr_0015, b.nr_0016, b.nr_0017, b.nr_0018, b.nr_0019, b.nr_0020, 
            b.nr_0021, b.nr_0022, b.nr_0023, b.nr_0024, b.nr_0025, b.nr_0026, b.nr_0027
        )';
        
        v_ssql_t2 := v_ssql_t2||v_ssql_t1||v_ssql_t3;
        dbms_output.put_line(v_ssql_t2);--打印拼接结果
        execute immediate v_ssql_t2;
        commit;
        
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_START_VC, V_PARTITION_NAME);
        --入库数量判断
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||') 
        group by s_date, ecgi having count(1)>1)'into v_insert_repeat;
            
        --重复率判断
        --正常情况下，按照CELL_NAME唯一
        dbms_output.put_line('表 OMC_NR_A 月级数据插入完成！时间戳：'||to_char(v_date_start,'yyyymmdd')||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');
        
    END PROC_OMC_NR_A;
    
    
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  --in：OMC_NR_8 /DT_CELL_NR
  --out：LC_INDEX_5G_DAY--多维度清单（非打点指标统计）
  PROCEDURE PROC_LC_INDEX_5G_DAY(V_DATE_THRESHOLD_START VARCHAR2) IS
    v_date_start  date;
    --v_date_end   date;
    --v_loop_log number := 0 ;
    v_insert_cnt   number;
    v_insert_repeat   number;
    v_partition_name varchar2(30);
    v_ssql varchar2(500);
    v_clean_flag number;
    --v_proc_end_flag number :=0;
        
    BEGIN
        --起止时间戳格式化
        select to_date(v_date_threshold_start,'yyyymmdd') into v_date_start from dual;--起始时间戳'yyyymmdd'格式化--2019/06/25
        --select v_date_start +1 into v_date_end from dual;--起始时间戳'yyyymmdd'格式化
            
        --从系统中获取待插入分区名
        select t.partition_name into v_partition_name from USER_TAB_PARTITIONS t 
        where t.table_name = 'LC_INDEX_5G_DAY' 
        and t.partition_name like 'P\_%' escape '\' --分区名严格遵守时，可忽略此行条件
        and regexp_substr(t.partition_name,'[^_]+',1,2,'i') = v_date_threshold_start; --regexp_substr('P_20190611','[^_]+',1,2,'i') → 20190611
        --或拼接待插入分区名
        --select 'P_'||V_DATE_THRESHOLD_START into v_partition_name from dual;
            
        --PLUS7暂未部署
        --PKG_MANAGE_SYSTEM_PLUS7_0.PROC_PARTITION_CLEANUP_RANGE('LRNOP','LC_INDEX_5G_DAY',v_date_start,v_date_start,'0','0');
            
        --分区数据清理
        /*v_ssql :=select 'select count(1) \*into v_clean_flag*\ from LC_INDEX_5G_DAY partition(' ||v_partition_name||');' into v_ssql from dual;
        execute immediate v_ssql;*/
        select count(1) into v_clean_flag from LC_INDEX_5G_DAY where start_time = v_date_start /*and start_time  < v_date_end*/;
        while v_clean_flag != 0 loop
          /*execute immediate 'DELETE FROM LC_INDEX_5G_DAY PARTITION(' || v_partition_name||')';
          commit;*/
          select
          'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_CLEANUP_RANGE('''|| table_name||''','''|| tm_grn ||''','''||v_date_threshold_start||''','''||v_date_threshold_start||''')'
          into v_ssql
          from FAST_DATA_PROCESS_TEMPLATE s
          where s.table_name = 'LC_INDEX_5G_DAY';
          execute immediate v_ssql;
          
          select count(1) into v_clean_flag from LC_INDEX_5G_DAY where start_time = v_date_start /*and start_time < v_date_end*/;
        end loop;
        
        --5G多维度清单数据插入
        execute immediate'            
        insert into LC_INDEX_5G_DAY
        select * from
        (
            --上海
            select 
            START_TIME,
            PROVINCE AS AREA,
            ''上海''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where PROVINCE is not null
            group by PROVINCE,START_TIME

            --行政区
            union all
            select 
            START_TIME,
            vendor_cell_id AS AREA,
            ''行政区''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where vendor_cell_id is not null
            group by vendor_cell_id,START_TIME
            
            

            --优化分区
            union all
            select 
            START_TIME,
            county AS AREA,
            ''优化分区''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where county is not null
            group by county,START_TIME

            --厂家
            union all
            select 
            START_TIME,
            vendor_id AS AREA,
            ''厂家''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where vendor_id is not null
            group by vendor_id,START_TIME

            --单元格
            union all
            select 
            START_TIME,
            reserved3 AS AREA,
            ''单元格''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where reserved3 is not null
            group by reserved3,START_TIME

            --区县分公司
            union all
            select 
            START_TIME,
            reserved8 AS AREA,
            ''区县分公司''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where reserved8 is not null
            group by reserved8,START_TIME
            
            --环线
            union all
            select 
            START_TIME,
            town_id AS AREA,
            ''环线''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where town_id is not null
            group by town_id,START_TIME
            
            --RNC
            union all
            select 
            START_TIME,
            RNC AS AREA,
            ''RNC''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where RNC is not null
            group by RNC,START_TIME

            --聚焦区域
            union all
            select 
            START_TIME,
            reserved4 AS AREA,
            ''聚焦区域''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where reserved4 is not null
            group by reserved4,START_TIME

            --频段
            union all
            select 
            START_TIME,
            reserved5 AS AREA,
            ''频段''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where reserved5 is not null
            group by reserved5,START_TIME

            --基站类型
            union all
            select 
            START_TIME,
            cover_type AS AREA,
            ''基站类型''AS AREA_LEVEL,
            avg(T1.n5_0001) user_down_speed,
            avg(T1.n5_0002) user_up_speed,
            avg(T1.n5_0003) cell_down_speed,
            avg(T1.n5_0004) cell_up_speed,
            avg(T1.n5_0005) down_rb_avail,
            avg(T1.n5_0006) up_rb_avail,
            round(sum(T1.n5_0007),2) down_flow_gb,
            round(sum(T1.n5_0008),2) up_flow_gb,
            avg(T1.n5_0009) user_duration_time
            from
            (
              select * from omc_nr_8 partition('||v_partition_name||') omc 
              left join
              dt_cell_nr dt
              on omc.cell_name=dt.PRE_FIELD5
            )T1--天级指标
            where cover_type is not null
            group by cover_type,START_TIME
        )';
        commit;

            
        --入库数量判断
        select count(1) into v_insert_cnt from LC_INDEX_5G_DAY t 
        where t.start_time = v_date_start;
                
        --重复率判断
        --正常情况下，按照CELL_NAME唯一
        select count(1) into v_insert_repeat from
        (
               select count(1) from LC_INDEX_5G_DAY t 
               where t.start_time  = v_date_start and area is not null/*and t.s_date< v_date_end*/ 
               group by t.area having count(1)>1
        );
        dbms_output.put_line('表 LC_INDEX_5G_DAY 天级数据插入完成！时间戳：'||to_char(v_date_start,'yyyymmdd')||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');
            
            
    END PROC_LC_INDEX_5G_DAY;

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  --5G基表数据周期性汇聚激活
  PROCEDURE ACTIVE_OMC_NR_389A_AUTO AS
    V_DATE_START  varchar2(15);
    V_DATE_START_8  varchar2(15);--天表任务激活时间戳    
    V_DATE_START_3  varchar2(5);--天表任务激活时间戳    
    V_DATE_HOUR number;
    V_DATE_MONDAY date;
    V_DATE_MONTH   date;
    v_loop_log number := 0 ;

    BEGIN
        --起止时间戳格式化，时间自动化，读取时间：sysdate--2019/07/01
        --延迟3小时所在天
        V_DATE_START :=  to_char(sysdate - numtodsinterval(3, 'hour'),'yyyymmdd');--20191022
        --延迟3小时所在小时
        V_DATE_HOUR  := /*23;*/ to_number(to_char(sysdate - numtodsinterval(3, 'hour'), 'hh24'));--now：10:21 --->  8
        --延迟1天
        V_DATE_START_8 :=  to_char(sysdate -1, 'yyyymmdd');--20191022
        V_DATE_START_3 :=  to_char(sysdate, 'hh24');--20191022
        
        --3
        --PKG_LC_OMC_NR_CELL.PROC_OMC_NR_3(V_DATE_START, V_DATE_HOUR);
        --v_loop_log := v_loop_log +1;
        
        --8
        if V_DATE_START_3 = 7
            then
                PKG_LC_OMC_NR_CELL.PROC_OMC_NR_8(V_DATE_START_8);
                v_loop_log := v_loop_log +1;
                --dbms_output.put_line('NR小区天级区域数量统计任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'.');
        end if;
        
        /*PKG_LC_OMC_NR_CELL.PROC_LC_INDEX_5G_DAY(V_DATE_START);
        v_loop_log := v_loop_log +1;*/
        
        --周/月汇聚触发时间戳保存
        --V_DATE_START :=  to_char(sysdate,'yyyymmdd');--20191122
        --每周周一汇聚上周的周级数据
        V_DATE_MONDAY := trunc(next_day(sysdate,'星期一'))-7;--2019/11/18
        --每月首日汇聚上月的月级数据
        V_DATE_MONTH := trunc(sysdate, 'mm');--20191101
        if  trunc(sysdate) = V_DATE_MONDAY--每周周一执行上周周汇聚--2019/11/18 = 2019/11/18
          then 
            PKG_LC_OMC_NR_CELL.PROC_OMC_NR_9(to_char(V_DATE_MONDAY-7, 'yyyymmdd'));--in：20191118 执行汇聚时间都会在包内转换为上周周一：20191111
            v_loop_log := v_loop_log +1;
        elsif  trunc(sysdate) = V_DATE_MONTH--每月一号执行上月月级汇聚--2019/11/01 = 2019/11/01
          then 
            PKG_LC_OMC_NR_CELL.PROC_OMC_NR_A(to_char(V_DATE_MONTH-1, 'yyyymmdd'));--in：20191101 执行汇聚时间会在包内转换为上月首日：20191031
            v_loop_log := v_loop_log +1;
        end if;

        dbms_output.put_line('5G小区天/周/月基表汇聚任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
      
    END ACTIVE_OMC_NR_389A_AUTO;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
  --5G基表数据周期性汇聚激活  
  PROCEDURE ACTIVE_OMC_NR_389A_SUPPLEMENT(V_DATE_THRESHOLD_START VARCHAR2) AS
    V_DATE_MONDAY date;
    V_DATE_MONTH   date;
    v_loop_log number := 0 ;

    BEGIN
        --周级数据补偿：补偿日期所在周的周级数据
        --月级数据补偿：补偿日期所在月的月级数据
        V_DATE_MONDAY := trunc(next_day(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd'),'星期一'))-7;--20190624
        V_DATE_MONTH := trunc(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd'), 'mm');--20190601
    
        --3
        PKG_LC_OMC_NR_CELL.PROC_OMC_NR_3(V_DATE_THRESHOLD_START, '');
        v_loop_log := v_loop_log +1;
        
        --8
        PKG_LC_OMC_NR_CELL.PROC_OMC_NR_8(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;
        
        /*PKG_LC_OMC_NR_CELL.PROC_LC_INDEX_5G_DAY(V_DATE_START);
        v_loop_log := v_loop_log +1;*/
        
        --9
        PKG_LC_OMC_NR_CELL.PROC_OMC_NR_9(V_DATE_MONDAY);--in：20190701 执行汇聚时间都会在包内转换为上周周一：20190624
        v_loop_log := v_loop_log +1;
        
         --A
        PKG_LC_OMC_NR_CELL.PROC_OMC_NR_A(V_DATE_MONTH);--in：20190701 执行汇聚时间都会在包内转换为上周周一：20190624
        v_loop_log := v_loop_log +1;

        dbms_output.put_line('5G小区天/周/月基表补偿任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
      
    END ACTIVE_OMC_NR_389A_SUPPLEMENT;

END PKG_LC_OMC_NR_CELL;
/

