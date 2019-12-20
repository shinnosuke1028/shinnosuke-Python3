CREATE OR REPLACE PACKAGE BODY PKG_LC_OMC_BUSY AS
--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
    --in：OMC_UMTS_3
    --out：OMC_UMTS_3_BUSY
    PROCEDURE PROC_OMC_UMTS_BUSY(V_DATE_THRESHOLD_START VARCHAR2) IS --小区小时指标
    -- v_high_value_vc                 varchar2(20);
    V_PARTITION_NAME                varchar2(50);
    V_TBNAME                        varchar2(100);
    --V_DATE_THRESHOLD_END    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd');--20190927
    SQL_1                   varchar2(4000);

    --统计明细
    v_insert_cnt   number;
    v_insert_repeat   number;
    --v_ssql varchar2(500);
    v_clean_flag number;
    --i_date_hour varchar2(20);
    v_timeout date:= sysdate;

    j number := 0;

    --游标明细
    TYPE OMC_UMTS_TYPE IS TABLE OF OMC_UMTS_3%ROWTYPE INDEX BY BINARY_INTEGER;
    OMC_UMTS_TAP OMC_UMTS_TYPE;

    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 1000;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;

    TYPE CurTyp IS REF CURSOR;
    CUR_SQL_1  CurTyp;

    BEGIN
        V_TBNAME := 'OMC_UMTS_3_BUSY';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);
        if V_PARTITION_NAME <> 'NULL'
            then
                --天级清理
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
                while v_clean_flag !=0 loop
                    PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX(V_TBNAME, '2', V_DATE_THRESHOLD_START, V_DATE_THRESHOLD_START);
                    execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
                end loop;  
        end if;

    
        V_TBNAME := 'OMC_UMTS_3';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);

        SQL_1 := '
        select
        s_date, s_month, s_week, s_day, s_hour, start_time, lac_id, cell_id, vendor,
        n3_0001, n3_0002, n3_0003, n3_0004, n3_0005, n3_0006, n3_0007, n3_0008, n3_0009,
        n3_0010, n3_0011, n3_0012, n3_0013, n3_0014, n3_0015, n3_0016, n3_0017, n3_0018, n3_0019, n3_0020, lac_ci
        from
        (
            select
            s_date, s_hour, lac_id, cell_id,
            round((50*3600*n3_0009+1.2*8*n3_0019),2) ll,
            row_number() over(partition by s_date, lac_id, cell_id order by (50*3600*n3_0009+1.2*8*n3_0019) desc, s_hour desc ) as seq,
            s_month, s_week, s_day, start_time, vendor, lac_ci,
            round(n3_0001, 2) n3_0001, round(n3_0002, 2) n3_0002, round(n3_0003, 2) n3_0003, round(n3_0004, 2) n3_0004,
            round(n3_0005, 2) n3_0005, round(n3_0006, 2) n3_0006, round(n3_0007, 2) n3_0007, round(n3_0008, 2) n3_0008,
            round(n3_0009, 2) n3_0009, round(n3_0010, 2) n3_0010, round(n3_0011, 2) n3_0011, round(n3_0012, 2) n3_0012,
            round(n3_0013, 2) n3_0013, round(n3_0014, 2) n3_0014, round(n3_0015, 2) n3_0015, round(n3_0016, 2) n3_0016,
            round(n3_0017, 2) n3_0017, round(n3_0018, 2) n3_0018, round(n3_0019, 2) n3_0019, round(n3_0020, 2) n3_0020
            from
            (
                select t.* from OMC_UMTS_3 partition('||V_PARTITION_NAME||') t --where lac_id||''-''||cell_id = ''43009-12122''
            )
        )where seq = 1';
        --dbms_output.put_line('SQL_1: '||SQL_1);
        --准备工作完成
        
        OPEN CUR_SQL_1 FOR SQL_1;
        LOOP
            FETCH CUR_SQL_1 BULK COLLECT INTO OMC_UMTS_TAP LIMIT V_LIMIT;
            EXIT WHEN OMC_UMTS_TAP.count = 0 or round(to_number(sysdate - v_timeout) * 24 * 60) >= 10;
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条


            FOR I IN OMC_UMTS_TAP.FIRST .. OMC_UMTS_TAP.LAST
            LOOP
                j := j + 1;
                --dbms_output.put_line('OMC_UMTS_TAP(I): '||OMC_UMTS_TAP(I).s_hour);
                --执行插入
                execute immediate 'insert /*+append*/ into OMC_UMTS_3_BUSY
                values(
                :s_date, :s_month, :s_week, :s_day, 
                :s_hour, :start_time, :lac_id, :cell_id, :vendor, 
                :n3_0001, :n3_0002, :n3_0003, :n3_0004, :n3_0005, 
                :n3_0006, :n3_0007, :n3_0008, :n3_0009, :n3_0010, 
                :n3_0011, :n3_0012, :n3_0013, :n3_0014, :n3_0015, 
                :n3_0016, :n3_0017, :n3_0018, :n3_0019, :n3_0020, 
                :lac_ci
                )'
                using
                OMC_UMTS_TAP(I).s_date, OMC_UMTS_TAP(I).s_month, OMC_UMTS_TAP(I).s_week, OMC_UMTS_TAP(I).s_day, 
                OMC_UMTS_TAP(I).s_hour, OMC_UMTS_TAP(I).start_time, OMC_UMTS_TAP(I).lac_id, OMC_UMTS_TAP(I).cell_id, OMC_UMTS_TAP(I).vendor, 
                OMC_UMTS_TAP(I).n3_0001, OMC_UMTS_TAP(I).n3_0002, OMC_UMTS_TAP(I).n3_0003, OMC_UMTS_TAP(I).n3_0004, OMC_UMTS_TAP(I).n3_0005, 
                OMC_UMTS_TAP(I).n3_0006, OMC_UMTS_TAP(I).n3_0007, OMC_UMTS_TAP(I).n3_0008, OMC_UMTS_TAP(I).n3_0009, OMC_UMTS_TAP(I).n3_0010, 
                OMC_UMTS_TAP(I).n3_0011, OMC_UMTS_TAP(I).n3_0012, OMC_UMTS_TAP(I).n3_0013, OMC_UMTS_TAP(I).n3_0014, OMC_UMTS_TAP(I).n3_0015, 
                OMC_UMTS_TAP(I).n3_0016, OMC_UMTS_TAP(I).n3_0017, OMC_UMTS_TAP(I).n3_0018, OMC_UMTS_TAP(I).n3_0019, OMC_UMTS_TAP(I).n3_0020, 
                OMC_UMTS_TAP(I).lac_ci
                ;
                if mod(j, 100)=0 --commit every 100 times
                    then commit;
                end if;

                --超时判定
                if round(to_number(sysdate - v_timeout) * 24 * 60) >= 10 then return;
                end if;

            END LOOP;
        END LOOP;
        COMMIT;
        CLOSE CUR_SQL_1;
        dbms_output.put_line('j: '||j);
        
        V_TBNAME := 'OMC_UMTS_3_BUSY';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, v_partition_name);

        --入库数量判断
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||') ' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||V_TBNAME||' partition('||v_partition_name||') 
        group by s_date, lac_id, cell_id having count(1)>1)'into v_insert_repeat;
        dbms_output.put_line('表 '||v_tbname||' 忙时数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');
    END PROC_OMC_UMTS_BUSY;

--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
    --in：OMC_LTE_3
    --out：OMC_LTE_3_BUSY
    PROCEDURE PROC_OMC_LTE_BUSY(V_DATE_THRESHOLD_START VARCHAR2) IS --小区小时指标
    -- v_high_value_vc                 varchar2(20);
    V_PARTITION_NAME                varchar2(50);
    V_TBNAME                        varchar2(100);
    --V_DATE_THRESHOLD_END    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd');--20190927
    SQL_1                   varchar2(4000);

    --统计明细
    v_insert_cnt   number;
    v_insert_repeat   number;
    --v_ssql varchar2(500);
    v_clean_flag number;
    --i_date_hour varchar2(20);
    v_timeout date:= sysdate;

    j number := 0;

    --游标明细
    TYPE OMC_LTE_TYPE IS TABLE OF OMC_LTE_3%ROWTYPE INDEX BY BINARY_INTEGER;
    OMC_LTE_TAP OMC_LTE_TYPE;

    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 1000;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;

    TYPE CurTyp IS REF CURSOR;
    CUR_SQL_1  CurTyp;

    BEGIN
        V_TBNAME := 'OMC_LTE_3_BUSY';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, v_partition_name);
        if V_PARTITION_NAME <> 'NULL'
            then
                --天级清理
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
                while v_clean_flag !=0 loop
                    PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX(V_TBNAME, '2', V_DATE_THRESHOLD_START, V_DATE_THRESHOLD_START);
                    execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
                end loop;  
        end if;
    
        V_TBNAME := 'OMC_LTE_3';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, v_partition_name);

        SQL_1 := '
        select s_date, s_month, s_week, s_day, s_hour, start_time, enb_id, cell_id, tac, ecgi, vendor, 
        n4_0001, n4_0002, n4_0003, n4_0004, n4_0005, n4_0006, n4_0007, n4_0008, n4_0009, 
        n4_0010, n4_0011, n4_0012, n4_0013, n4_0014, n4_0015, n4_0016, n4_0017, n4_0018, n4_0019, 
        n4_0020, n4_0021, n4_0022, n4_0023, n4_0024, n4_0025, n4_0026, n4_0027, n4_0028, n4_0029, 
        n4_0030, n4_0031, n4_0032, n4_0033, n4_0034, n4_0035, n4_0036, n4_0037, n4_0038, n4_0039, 
        n4_0040, n4_0041, n4_0042, n4_0043, n4_0044, n4_0045, n4_0046, n4_0047, n4_0048, n4_0049, 
        n4_0050, avg_cqi,  n4_0051, n4_0052, n4_0053, n4_0054, n4_0055, n4_0056, n4_0057, n4_0058, n4_0059, 
        n4_0060, n4_0061, n4_0062, n4_0063, n4_0064, n4_0065, n4_0066, n4_0067, n4_0068, n4_0069, 
        n4_0070, n4_0071, n4_0072, n4_0073, n4_0074, n4_0075, n4_0076, n4_0077, n4_0078, n4_0079, 
        n4_0080, n4_0081, n4_0082, n4_0083, n4_0084
        from
        (
            select 
            s_date, s_hour, ecgi,
            --(decode(n4_0027,null,0,n4_0027)+decode(n4_0050,null,0,n4_0050)) ll,
            row_number() over(partition by s_date,ecgi order by (decode(n4_0027,null,0,n4_0027)+decode(n4_0050,null,0,n4_0050)) desc, s_hour desc ) as seq, 
            s_month, s_week, s_day, start_time, enb_id, cell_id, tac, vendor, 
            n4_0001, n4_0002, n4_0003, n4_0004, n4_0005, n4_0006, n4_0007, n4_0008, n4_0009, 
            n4_0010, n4_0011, n4_0012, n4_0013, n4_0014, n4_0015, n4_0016, n4_0017, n4_0018, n4_0019, 
            n4_0020, n4_0021, n4_0022, n4_0023, n4_0024, n4_0025, n4_0026, n4_0027, n4_0028, n4_0029, 
            n4_0030, n4_0031, n4_0032, n4_0033, n4_0034, n4_0035, n4_0036, n4_0037, n4_0038, n4_0039, 
            n4_0040, n4_0041, n4_0042, n4_0043, n4_0044, n4_0045, n4_0046, n4_0047, n4_0048, n4_0049, 
            n4_0050, avg_cqi,  n4_0051, n4_0052, n4_0053, n4_0054, n4_0055, n4_0056, n4_0057, n4_0058, n4_0059, 
            n4_0060, n4_0061, n4_0062, n4_0063, n4_0064, n4_0065, n4_0066, n4_0067, n4_0068, n4_0069, 
            n4_0070, n4_0071, n4_0072, n4_0073, n4_0074, n4_0075, n4_0076, n4_0077, n4_0078, n4_0079, 
            n4_0080, n4_0081, n4_0082, n4_0083, n4_0084
            from
            (
                select t.* from OMC_LTE_3 partition('||V_PARTITION_NAME||') t --where ecgi = ''8388619''
            )
        )where seq = 1';
        --dbms_output.put_line('SQL_1: '||SQL_1);
        --准备工作完成
        
        OPEN CUR_SQL_1 FOR SQL_1;
        LOOP
            FETCH CUR_SQL_1 BULK COLLECT INTO OMC_LTE_TAP LIMIT V_LIMIT;
            EXIT WHEN OMC_LTE_TAP.count = 0 or round(to_number(sysdate - v_timeout) * 24 * 60) >= 10;
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条


            FOR I IN OMC_LTE_TAP.FIRST .. OMC_LTE_TAP.LAST
            LOOP
                j := j + 1;
                --dbms_output.put_line('OMC_LTE_TAP(I): '||OMC_LTE_TAP(I).s_hour);
                --执行插入
                execute immediate 'insert /*+append*/ into OMC_LTE_3_BUSY
                values(
                :s_date, :s_month, :s_week, :s_day, 
                :s_hour, :start_time, :enb_id, :cell_id, 
                :tac, :ecgi, :vendor, 
                :n4_0001, :n4_0002, :n4_0003, :n4_0004, :n4_0005, :n4_0006, :n4_0007, :n4_0008, :n4_0009, 
                :n4_0010, :n4_0011, :n4_0012, :n4_0013, :n4_0014, :n4_0015, :n4_0016, :n4_0017, :n4_0018, :n4_0019, 
                :n4_0020, :n4_0021, :n4_0022, :n4_0023, :n4_0024, :n4_0025, :n4_0026, :n4_0027, :n4_0028, :n4_0029, 
                :n4_0030, :n4_0031, :n4_0032, :n4_0033, :n4_0034, :n4_0035, :n4_0036, :n4_0037, :n4_0038, :n4_0039, 
                :n4_0040, :n4_0041, :n4_0042, :n4_0043, :n4_0044, :n4_0045, :n4_0046, :n4_0047, :n4_0048, :n4_0049, 
                :n4_0050, :avg_cqi,  :n4_0051, :n4_0052, :n4_0053, :n4_0054, :n4_0055, :n4_0056, :n4_0057, :n4_0058, :n4_0059, 
                :n4_0060, :n4_0061, :n4_0062, :n4_0063, :n4_0064, :n4_0065, :n4_0066, :n4_0067, :n4_0068, :n4_0069, 
                :n4_0070, :n4_0071, :n4_0072, :n4_0073, :n4_0074, :n4_0075, :n4_0076, :n4_0077, :n4_0078, :n4_0079, 
                :n4_0080, :n4_0081, :n4_0082, :n4_0083, :n4_0084
                )'
                using
                OMC_LTE_TAP(I).s_date, OMC_LTE_TAP(I).s_month, OMC_LTE_TAP(I).s_week, OMC_LTE_TAP(I).s_day, 
                OMC_LTE_TAP(I).s_hour, OMC_LTE_TAP(I).start_time, OMC_LTE_TAP(I).enb_id, OMC_LTE_TAP(I).cell_id, 
                OMC_LTE_TAP(I).tac, OMC_LTE_TAP(I).ecgi, OMC_LTE_TAP(I).vendor, 
                OMC_LTE_TAP(I).n4_0001, OMC_LTE_TAP(I).n4_0002, OMC_LTE_TAP(I).n4_0003, OMC_LTE_TAP(I).n4_0004, OMC_LTE_TAP(I).n4_0005, OMC_LTE_TAP(I).n4_0006, OMC_LTE_TAP(I).n4_0007, OMC_LTE_TAP(I).n4_0008, OMC_LTE_TAP(I).n4_0009, 
                OMC_LTE_TAP(I).n4_0010, OMC_LTE_TAP(I).n4_0011, OMC_LTE_TAP(I).n4_0012, OMC_LTE_TAP(I).n4_0013, OMC_LTE_TAP(I).n4_0014, OMC_LTE_TAP(I).n4_0015, OMC_LTE_TAP(I).n4_0016, OMC_LTE_TAP(I).n4_0017, OMC_LTE_TAP(I).n4_0018, OMC_LTE_TAP(I).n4_0019, 
                OMC_LTE_TAP(I).n4_0020, OMC_LTE_TAP(I).n4_0021, OMC_LTE_TAP(I).n4_0022, OMC_LTE_TAP(I).n4_0023, OMC_LTE_TAP(I).n4_0024, OMC_LTE_TAP(I).n4_0025, OMC_LTE_TAP(I).n4_0026, OMC_LTE_TAP(I).n4_0027, OMC_LTE_TAP(I).n4_0028, OMC_LTE_TAP(I).n4_0029, 
                OMC_LTE_TAP(I).n4_0030, OMC_LTE_TAP(I).n4_0031, OMC_LTE_TAP(I).n4_0032, OMC_LTE_TAP(I).n4_0033, OMC_LTE_TAP(I).n4_0034, OMC_LTE_TAP(I).n4_0035, OMC_LTE_TAP(I).n4_0036, OMC_LTE_TAP(I).n4_0037, OMC_LTE_TAP(I).n4_0038, OMC_LTE_TAP(I).n4_0039, 
                OMC_LTE_TAP(I).n4_0040, OMC_LTE_TAP(I).n4_0041, OMC_LTE_TAP(I).n4_0042, OMC_LTE_TAP(I).n4_0043, OMC_LTE_TAP(I).n4_0044, OMC_LTE_TAP(I).n4_0045, OMC_LTE_TAP(I).n4_0046, OMC_LTE_TAP(I).n4_0047, OMC_LTE_TAP(I).n4_0048, OMC_LTE_TAP(I).n4_0049, 
                OMC_LTE_TAP(I).n4_0050, OMC_LTE_TAP(I).avg_cqi,  OMC_LTE_TAP(I).n4_0051, OMC_LTE_TAP(I).n4_0052, OMC_LTE_TAP(I).n4_0053, OMC_LTE_TAP(I).n4_0054, OMC_LTE_TAP(I).n4_0055, OMC_LTE_TAP(I).n4_0056, OMC_LTE_TAP(I).n4_0057, OMC_LTE_TAP(I).n4_0058, OMC_LTE_TAP(I).n4_0059, 
                OMC_LTE_TAP(I).n4_0060, OMC_LTE_TAP(I).n4_0061, OMC_LTE_TAP(I).n4_0062, OMC_LTE_TAP(I).n4_0063, OMC_LTE_TAP(I).n4_0064, OMC_LTE_TAP(I).n4_0065, OMC_LTE_TAP(I).n4_0066, OMC_LTE_TAP(I).n4_0067, OMC_LTE_TAP(I).n4_0068, OMC_LTE_TAP(I).n4_0069, 
                OMC_LTE_TAP(I).n4_0070, OMC_LTE_TAP(I).n4_0071, OMC_LTE_TAP(I).n4_0072, OMC_LTE_TAP(I).n4_0073, OMC_LTE_TAP(I).n4_0074, OMC_LTE_TAP(I).n4_0075, OMC_LTE_TAP(I).n4_0076, OMC_LTE_TAP(I).n4_0077, OMC_LTE_TAP(I).n4_0078, OMC_LTE_TAP(I).n4_0079, 
                OMC_LTE_TAP(I).n4_0080, OMC_LTE_TAP(I).n4_0081, OMC_LTE_TAP(I).n4_0082, OMC_LTE_TAP(I).n4_0083, OMC_LTE_TAP(I).n4_0084
                ;
                if mod(j, 100)=0 --commit every 100 times
                    then commit;
                end if;

                --超时判定
                if round(to_number(sysdate - v_timeout) * 24 * 60) >= 10 then return;
                end if;

            END LOOP;
        END LOOP;
        COMMIT;
        CLOSE CUR_SQL_1;
        dbms_output.put_line('j: '||j);
        
        V_TBNAME := 'OMC_LTE_3_BUSY';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);

        --入库数量判断
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||') ' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||') 
        group by s_date, ecgi having count(1)>1)'into v_insert_repeat;
        dbms_output.put_line('表 '||v_tbname||' 忙时数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');


    END PROC_OMC_LTE_BUSY;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --OMC忙时自动任务
    PROCEDURE ACTIVE_OMC_BUSY_AUTO AS
    V_DATE_START  varchar2(15);--天表任务激活时间戳    
    V_DT_DATE_START  varchar2(15);--天表任务激活时间戳    
    v_loop_log number := 0;
    /*v_inside_loop_log number := 0;
    v_tbname varchar2(50);
    v_partition_name varchar2(30);
    v_exsit_flag number := 0;
    v_pkg_name varchar2(200);*/
    BEGIN
        --起止时间戳格式化，时间自动化，读取时间：sysdate--2019/11/13
        V_DATE_START :=  to_char(sysdate - 1 ,'yyyymmdd');--20191112
        V_DT_DATE_START :=  to_char(sysdate ,'yyyymmdd');--20191113
        
        --每日执行昨日天级汇聚
        /*v_tbname := 'LC_INDEX_VOLTE_8';
        v_pkg_name := 'PKG_LC_INDEX_VOLTE_CELL.PROC_LC_INDEX_VOLTE_8';
         */
        PKG_LC_OMC_BUSY.PROC_OMC_LTE_BUSY(V_DATE_START);
        v_loop_log := v_loop_log +1;

        PKG_LC_OMC_BUSY.PROC_OMC_UMTS_BUSY(V_DATE_START);
        v_loop_log := v_loop_log +1;

        dbms_output.put_line('OMC忙时自动汇聚任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
        dbms_output.put_line('****------------------------------------------------------------------****
        ');
        
        PKG_LC_OMC_BUSY.PROC_OSS_DT_CELL_L(V_DT_DATE_START);
        v_loop_log := 1;

        PKG_LC_OMC_BUSY.PROC_OSS_DT_CELL_W(V_DT_DATE_START);
        v_loop_log := v_loop_log +1;

        dbms_output.put_line('DT时间工参任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
        dbms_output.put_line('****------------------------------------------------------------------****
        ');

    END ACTIVE_OMC_BUSY_AUTO;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --OMC忙时自动任务
    PROCEDURE ACTIVE_OMC_BUSY_SUPPLEMENT(V_DATE_THRESHOLD_START VARCHAR2) AS
    --V_DATE_START  varchar2(15);--天表任务激活时间戳    
    v_loop_log number := 0;
    /*v_inside_loop_log number := 0;
    v_tbname varchar2(50);
    v_partition_name varchar2(30);
    v_exsit_flag number := 0;
    v_pkg_name varchar2(200);*/
    BEGIN
        --每日执行昨日天级汇聚
        /*v_tbname := 'LC_INDEX_VOLTE_8';
        v_pkg_name := 'PKG_LC_INDEX_VOLTE_CELL.PROC_LC_INDEX_VOLTE_8';
         */
        PKG_LC_OMC_BUSY.PROC_OMC_LTE_BUSY(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;

        PKG_LC_OMC_BUSY.PROC_OMC_UMTS_BUSY(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;

        dbms_output.put_line('OMC忙时自动汇聚任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
        dbms_output.put_line('****------------------------------------------------------------------****
        ');

    END ACTIVE_OMC_BUSY_SUPPLEMENT; 

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --in：OMC_UMTS_8, DT_CELL_W
    --out：OSS_DT_CELL_W
    PROCEDURE PROC_OSS_DT_CELL_W(V_DATE_THRESHOLD_START VARCHAR2) AS
    V_PARTITION_NAME                varchar2(50);
    V_TBNAME                        varchar2(100);
    --V_DATE_THRESHOLD_END    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd');--20190927
    SQL_1                   varchar2(4000);
    --统计明细1
    v_insert_cnt   number;
    --v_insert_repeat   number;
    --v_ssql varchar2(500);
    v_clean_flag number;
    v_timeout date:= sysdate;
    v_date_before varchar2(10) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 1, 'yyyymmdd');

    j number := 0;
 
    --游标明细
    TYPE NESTED_EXPORT_TYPE IS TABLE OF OSS_DT_CELL_W%ROWTYPE INDEX BY BINARY_INTEGER;
    OSS_DT_TAB NESTED_EXPORT_TYPE;

    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 1000;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;

    TYPE CurTyp IS REF CURSOR;
    CUR_SQL_1  CurTyp;    
    
    BEGIN
       --程序超时判定的起始时间设置
        v_timeout := sysdate;
        --清理
        V_TBNAME := 'OSS_DT_CELL_W';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, v_partition_name);
        if v_partition_name <> 'NULL'
            then
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
                while v_clean_flag !=0 loop
                    PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX(V_TBNAME, '2', V_DATE_THRESHOLD_START, V_DATE_THRESHOLD_START);
                    execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
                end loop;  
        end if;
        
        --注意，这里使用的是前一天的底层数据跑当天的数据
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OMC_UMTS_8', v_date_before, v_partition_name); 
        
        SQL_1 := '
        select 
        omc.lac_id , omc.cell_id , province, city, 
        country, factory, grid, company, 
        loop_line, rnc, pre_field1, pre_field2, pre_field3, pre_field4 ||''~''||omc.lac_ci as pre_field4,
        pre_field5, life, lon, lat, omc.lac_ci, omc.s_date + 1
        from
        (
        select * from OMC_UMTS_8 partition('||v_partition_name||')
        )omc
        left join
        dt_cell_w dt
        on omc.lac_ci = dt.lac_ci
        ';
        
        OPEN CUR_SQL_1 FOR SQL_1;
        LOOP
            FETCH CUR_SQL_1 BULK COLLECT INTO OSS_DT_TAB LIMIT V_LIMIT;
            EXIT WHEN OSS_DT_TAB.count = 0 or round(to_number(sysdate - v_timeout) * 24 * 60) >= 10;
            V_COUNTER := V_COUNTER + 1; 
            

            FOR I IN OSS_DT_TAB.FIRST .. OSS_DT_TAB.LAST
            LOOP
                j := j + 1;
                execute immediate 'insert /*+append*/ into OSS_DT_CELL_W
                values(
                :LAC, :CI, :PROVINCE, :CITY, :COUNTRY, 
                :FACTORY, :GRID, :COMPANY, :LOOP_LINE, :RNC, 
                :PRE_FIELD1, :PRE_FIELD2, :PRE_FIELD3, :PRE_FIELD4, 
                :PRE_FIELD5, :LIFE, :LON, :LAT, :LAC_CI, :S_DATE
                )'
                using
                OSS_DT_TAB(I).LAC, OSS_DT_TAB(I).CI, OSS_DT_TAB(I).PROVINCE, OSS_DT_TAB(I).CITY, OSS_DT_TAB(I).COUNTRY, 
                OSS_DT_TAB(I).FACTORY, OSS_DT_TAB(I).GRID, OSS_DT_TAB(I).COMPANY, OSS_DT_TAB(I).LOOP_LINE, OSS_DT_TAB(I).RNC, 
                OSS_DT_TAB(I).PRE_FIELD1, OSS_DT_TAB(I).PRE_FIELD2, OSS_DT_TAB(I).PRE_FIELD3, OSS_DT_TAB(I).PRE_FIELD4, 
                OSS_DT_TAB(I).PRE_FIELD5, OSS_DT_TAB(I).LIFE, OSS_DT_TAB(I).LON, OSS_DT_TAB(I).LAT, OSS_DT_TAB(I).LAC_CI, OSS_DT_TAB(I).S_DATE;
                
                if mod(j, 500)=0 --commit every 100 times
                    then commit;
                end if;

                --超时判定
                if round(to_number(sysdate - v_timeout) * 24 * 60) >= 10 then return;
                end if;

            END LOOP;
        END LOOP;
        COMMIT;
        CLOSE CUR_SQL_1;
        
        dbms_output.put_line('j: '||j);

        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OSS_DT_CELL_W', V_DATE_THRESHOLD_START, v_partition_name);

        --入库数量判断
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_insert_cnt;
        --重复率判断
        /*execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        group by s_date, lac_ci, style having count(1)>1)'into v_insert_repeat;*/
        dbms_output.put_line('表 '||V_TBNAME||' 天级数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||' 行.
        ');
    END PROC_OSS_DT_CELL_W;
    

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --in：OMC_LTE_8, DT_CELL_L
    --out：OSS_DT_CELL_L
    PROCEDURE PROC_OSS_DT_CELL_L(V_DATE_THRESHOLD_START VARCHAR2) AS
    V_PARTITION_NAME                varchar2(50);
    V_TBNAME                        varchar2(100);
    --V_DATE_THRESHOLD_END    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd');--20190927
    SQL_1                   varchar2(4000);

    --统计明细
    v_insert_cnt   number;
    --v_insert_repeat   number;
    --v_ssql varchar2(500);
    v_clean_flag number;
    --i_date_hour varchar2(20);
    v_timeout date:= sysdate;
    v_date_before varchar2(10) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 1, 'yyyymmdd');

    j number := 0;
    
    TYPE NESTED_EXPORT_TYPE IS TABLE OF OSS_DT_CELL_L%ROWTYPE INDEX BY BINARY_INTEGER;
    OSS_DT_TAB NESTED_EXPORT_TYPE;

    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 1000;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;

    TYPE CurTyp IS REF CURSOR;
    CUR_SQL_1  CurTyp;    
    
    BEGIN
       --程序超时判定的起始时间设置
        v_timeout := sysdate;
        --清理
        V_TBNAME := 'OSS_DT_CELL_L';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, v_partition_name);
        if v_partition_name <> 'NULL'
            then
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
                while v_clean_flag !=0 loop
                    PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX(V_TBNAME, '2', V_DATE_THRESHOLD_START, V_DATE_THRESHOLD_START);
                    execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
                end loop;  
        end if;
        
        --注意，这里使用的是前一天的底层数据跑当天的数据
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OMC_LTE_8', v_date_before, v_partition_name); 
        
        SQL_1 := '
        select oid, omc.enb_id, omc.cell_id, province, 
        vendor_cell_id, county, vendor_id, reserved3, 
        reserved8, town_id, rnc, reserved4, reserved5, 
        cover_type, pre_field4, 
        pre_field5||''~''||omc.ecgi as pre_field5, 
        life, lon, lat, 
        omc.ecgi, omc.s_date + 1 from
        (
            select * from OMC_LTE_8 partition('||v_partition_name||')
        )omc
        left join
        dt_cell_l dt
        on omc.ecgi = dt.ecgi
        ';
        
        OPEN CUR_SQL_1 FOR SQL_1;
        LOOP
            FETCH CUR_SQL_1 BULK COLLECT INTO OSS_DT_TAB LIMIT V_LIMIT;
            EXIT WHEN OSS_DT_TAB.count = 0 or round(to_number(sysdate - v_timeout) * 24 * 60) >= 10;
            V_COUNTER := V_COUNTER + 1; 
            

            FOR I IN OSS_DT_TAB.FIRST .. OSS_DT_TAB.LAST
            LOOP
                j := j + 1;
                execute immediate 'insert /*+ append */ into OSS_DT_CELL_L
                values(
                :OID, :ENB_ID, :CI, :PROVINCE, :VENDOR_CELL_ID, 
                :COUNTY, :VENDOR_ID, :RESERVED3, :RESERVED8, :TOWN_ID, 
                :RNC, :RESERVED4, :RESERVED5, :COVER_TYPE, :PRE_FIELD4, 
                :PRE_FIELD5, :LIFE, :LON, :LAT, :ECGI, :S_DATE
                )'
                using
                OSS_DT_TAB(I).OID, OSS_DT_TAB(I).ENB_ID, OSS_DT_TAB(I).CI, OSS_DT_TAB(I).PROVINCE, OSS_DT_TAB(I).VENDOR_CELL_ID, 
                OSS_DT_TAB(I).COUNTY, OSS_DT_TAB(I).VENDOR_ID, OSS_DT_TAB(I).RESERVED3, OSS_DT_TAB(I).RESERVED8, OSS_DT_TAB(I).TOWN_ID, 
                OSS_DT_TAB(I).RNC, OSS_DT_TAB(I).RESERVED4, OSS_DT_TAB(I).RESERVED5, OSS_DT_TAB(I).COVER_TYPE, OSS_DT_TAB(I).PRE_FIELD4, 
                OSS_DT_TAB(I).PRE_FIELD5, OSS_DT_TAB(I).LIFE, OSS_DT_TAB(I).LON, OSS_DT_TAB(I).LAT, OSS_DT_TAB(I).ECGI, OSS_DT_TAB(I).S_DATE;
                
                if mod(j, 500)=0 --commit every 100 times
                    then commit;
                end if;

                --超时判定
                if round(to_number(sysdate - v_timeout) * 24 * 60) >= 10 then return;
                end if;

            END LOOP;
        END LOOP;
        COMMIT;
        CLOSE CUR_SQL_1;
        
        dbms_output.put_line('j: '||j);

        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OSS_DT_CELL_L', V_DATE_THRESHOLD_START, v_partition_name);

        --入库数量判断
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_insert_cnt;
        --重复率判断
        /*execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        group by s_date, lac_ci, style having count(1)>1)'into v_insert_repeat;*/
        dbms_output.put_line('表 '||V_TBNAME||' 天级数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||' 行.
        ');
    END PROC_OSS_DT_CELL_L;



-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --in：OMC_LTE_8, DT_CELL_NB
    --out：OSS_DT_CELL_NB
    PROCEDURE PROC_OSS_DT_CELL_NB(V_DATE_THRESHOLD_START VARCHAR2) AS
        V_PARTITION_NAME                varchar2(50);
        V_TBNAME                        varchar2(100);
        --V_DATE_THRESHOLD_END    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd');--20190927
        SQL_1                   varchar2(4000);

        --统计明细
        v_insert_cnt   number;
        --v_insert_repeat   number;
        --v_ssql varchar2(500);
        v_clean_flag number;
        --i_date_hour varchar2(20);
        v_timeout date:= sysdate;
        v_date_before varchar2(10) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 1, 'yyyymmdd');

        j number := 0;

        TYPE NESTED_EXPORT_TYPE IS TABLE OF OSS_DT_CELL_NB%ROWTYPE INDEX BY BINARY_INTEGER;
        OSS_DT_TAB NESTED_EXPORT_TYPE;

        -- 定义了一个变量来作为limit的值
        V_LIMIT PLS_INTEGER := 1000;
        -- 定义变量来记录FETCH次数
        V_COUNTER INTEGER := 0;

        TYPE CurTyp IS REF CURSOR;
        CUR_SQL_1  CurTyp;

    BEGIN
        --程序超时判定的起始时间设置
        v_timeout := sysdate;
        --清理
        V_TBNAME := 'OSS_DT_CELL_NB';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);
        if V_PARTITION_NAME <> 'NULL'
        then
            execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
            while v_clean_flag !=0 loop
                    PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE_INDEX(V_TBNAME, '2', V_DATE_THRESHOLD_START, V_DATE_THRESHOLD_START);
                    execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
                end loop;
        end if;

        --注意，这里使用的是前一天的底层数据跑当天的数据
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OMC_LTE_8', v_date_before, V_PARTITION_NAME);

        SQL_1 := '
            select oid, omc.enb_id, omc.cell_id, province,
            vendor_cell_id, county, vendor_id, reserved3,
            reserved8, town_id, rnc, reserved4, reserved5,
            cover_type, pre_field4,
            pre_field5||''~''||omc.ecgi as pre_field5,
            life, lon, lat,
            omc.ecgi, omc.s_date + 1 from
            (
                select * from OMC_LTE_8 partition('||V_PARTITION_NAME||')
            )omc
            left join
            dt_cell_l dt
            on omc.ecgi = dt.ecgi
            ';

        OPEN CUR_SQL_1 FOR SQL_1;
        LOOP
            FETCH CUR_SQL_1 BULK COLLECT INTO OSS_DT_TAB LIMIT V_LIMIT;
            EXIT WHEN OSS_DT_TAB.count = 0 or round(to_number(sysdate - v_timeout) * 24 * 60) >= 10;
            V_COUNTER := V_COUNTER + 1;


            FOR I IN OSS_DT_TAB.FIRST .. OSS_DT_TAB.LAST
                LOOP
                    j := j + 1;
                    execute immediate 'insert /*+ append */ into OSS_DT_CELL_L
                                       values(
                                                 :OID, :ENB_ID, :CI, :PROVINCE, :VENDOR_CELL_ID,
                                                 :COUNTY, :VENDOR_ID, :RESERVED3, :RESERVED8, :TOWN_ID,
                                                 :RNC, :RESERVED4, :RESERVED5, :COVER_TYPE, :PRE_FIELD4,
                                                 :PRE_FIELD5, :LIFE, :LON, :LAT, :ECGI, :S_DATE
                                             )'
                        using
                        OSS_DT_TAB(I).OID, OSS_DT_TAB(I).ENB_ID, OSS_DT_TAB(I).CI, OSS_DT_TAB(I).PROVINCE, OSS_DT_TAB(I).VENDOR_CELL_ID,
                        OSS_DT_TAB(I).COUNTY, OSS_DT_TAB(I).VENDOR_ID, OSS_DT_TAB(I).RESERVED3, OSS_DT_TAB(I).RESERVED8, OSS_DT_TAB(I).TOWN_ID,
                        OSS_DT_TAB(I).RNC, OSS_DT_TAB(I).RESERVED4, OSS_DT_TAB(I).RESERVED5, OSS_DT_TAB(I).COVER_TYPE, OSS_DT_TAB(I).PRE_FIELD4,
                        OSS_DT_TAB(I).PRE_FIELD5, OSS_DT_TAB(I).LIFE, OSS_DT_TAB(I).LON, OSS_DT_TAB(I).LAT, OSS_DT_TAB(I).ECGI, OSS_DT_TAB(I).S_DATE;

                    if mod(j, 500)=0 --commit every 100 times
                    then commit;
                    end if;

                    --超时判定
                    if round(to_number(sysdate - v_timeout) * 24 * 60) >= 10 then return;
                    end if;

                END LOOP;
        END LOOP;
        COMMIT;
        CLOSE CUR_SQL_1;

        dbms_output.put_line('j: '||j);

        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE('OSS_DT_CELL_L', V_DATE_THRESHOLD_START, V_PARTITION_NAME);

        --入库数量判断
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_insert_cnt;
        --重复率判断
        /*execute immediate 'select count(1) from (select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')
        group by s_date, lac_ci, style having count(1)>1)'into v_insert_repeat;*/
        dbms_output.put_line('表 '||V_TBNAME||' 天级数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||' 行.
            ');
    END PROC_OSS_DT_CELL_NB;


END PKG_LC_OMC_BUSY;
/

