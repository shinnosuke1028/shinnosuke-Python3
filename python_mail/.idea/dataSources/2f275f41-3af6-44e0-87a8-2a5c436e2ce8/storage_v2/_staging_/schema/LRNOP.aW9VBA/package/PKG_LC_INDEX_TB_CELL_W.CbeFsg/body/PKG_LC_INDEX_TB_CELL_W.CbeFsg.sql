CREATE OR REPLACE PACKAGE BODY PKG_LC_INDEX_TB_CELL_W AS
    --in：OMC_UMTS_3 / DT_CELL_W
    --out：TB_LIST_CELL_HOUR_W
    PROCEDURE PROC_TB_LIST_CELL_HOUR_W(V_DATE_THRESHOLD_START VARCHAR2, V_DATE_HOUR VARCHAR2 := NULL) IS --小区小时指标
    --v_high_value_vc                 varchar2(20);
    v_partition_name                varchar2(50);
    --v_partition_name_7_ago        varchar2(50);
    --v_mr_partition_name           varchar2(50);
    --v_mr_partition_name_7_ago     varchar2(50);
    --V_DATE_THRESHOLD_START_7_AGO  varchar2(8);
    V_TBNAME                        varchar2(100);
    V_DATE_THRESHOLD_END            varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd');--20190927

    V_DATE_THRESHOLD_START_7_AGO    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 7, 'yyyymmdd'); --2019/10/03 - 7 = 2019/09/26 → 20190926
    V_DATE_THRESHOLD_END_7_AGO      varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 6, 'yyyymmdd'); --20190927
    --v_s_date_7                    date;
    --sql_2                         varchar2(4000);
    --V_DATE_THRESHOLD_HOUR         varchar2(20);

   /* v_insert_cnt   number;
    v_insert_repeat   number;*/
    v_ssql varchar2(500);
    v_clean_flag number;

    j number := 0;
    -- c_high_value varchar2(200);
    -- c_table_name varchar2(50);
    -- c_partition_name varchar2(80);

    type OMC_3_TYPE is record(
        S_DATE          date,
        S_HOUR          integer,
        LAC_ID          integer,
        CELL_ID         integer,
        LAC_CI          varchar2(50),
        W_DHL           number,
        W_DHL_BEF       number,
        W_DHL_NUM       number,
        W_DHL_NUM_BEF   number,
        W_DHL_D         number,
        W_DHL_D_BEF     number,
        W_JTL           number,
        W_JTL_BEF       number,
        W_JTL_N         number,
        W_JTL_N_BEF     number,
        W_JTL_D         number,
        W_JTL_D_BEF     number,
        W_JTL_QUEST     number,
        W_JTL_QUEST_BEF number,
        W_RTWP          number,
        W_RTWP_BEF      number,
        W_GCJ           number,
        W_GCJ_BEF       number,
        W_GCJ_N         number,
        W_GCJ_N_BEF     number,
        W_CJ_NUM        number,
        W_CJ_NUM_BEF    number,
        W_RQH_CGL       number,
        W_RQH_CGL_BEF   number,
        W_RQH_CGL_N     number,
        W_RQH_CGL_N_BEF number,
        W_RQH_QUEST     number,
        W_RQH_QUEST_BEF number,
        W_HWL           number,
        W_HWL_BEF       number,
        PROVINCE    varchar2(32),
        CITY        varchar2(32),
        COUNTRY     varchar2(64),
        FACTORY     varchar2(64),
        GRID        varchar2(64),
        COMPANY     varchar2(64),
        LOOP_LINE   varchar2(64),
        RNC         varchar2(64),
        PRE_FIELD1  varchar2(64),
        PRE_FIELD2  varchar2(64),
        PRE_FIELD3  varchar2(64),
        PRE_FIELD4  varchar2(256),
        PRE_FIELD5  varchar2(64),
        LIFE        varchar2(64)
    );
    --omc3_tmp_1 OMC_3_TYPE;

    -- 定义基于记录的嵌套表
    TYPE NESTED_OMC_TYPE IS TABLE OF OMC_3_TYPE;
    -- 声明集合变量
    OMC_TAB NESTED_OMC_TYPE;
    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 500;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;

    W_DHL_BADFLAG   varchar2(5) := 0;
    W_RTWP_BADFLAG  varchar2(5) := 0;
    W_GCJ_BADFLAG   varchar2(5) := 0;
    W_JTL_BADFLAG   varchar2(5) := 0;
    W_RQH_BADFLAG   varchar2(5) := 0;
    W_JTL_TBFLAG    varchar2(5) := 0;
    W_DHL_TBFLAG    varchar2(5) := 0;
    W_RTWP_TBFLAG   varchar2(5) := 0;
    W_GCJ_TBFLAG    varchar2(5) := 0;
    W_RQH_TBFLAG    varchar2(5) := 0;
    W_HWL_TBFLAG    varchar2(5) := 0;

    /*type partition_type is record(
        table_name      varchar2(200),
        partition_name  varchar2(50),
        high_value      varchar2(100)
    );
    partition_tmp partition_type;

    cursor cur_partition is --字典表内获取非标准分区的分区名
    select table_name,t.partition_name,t.high_value
    from USER_TAB_PARTITIONS t
    where table_name = V_TBNAME and partition_name not like 'P2%'--SYS_21313
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历*/

   /* type curtyp is ref cursor;
    cur_sql_2 curtyp;*/

    cursor cur_sql_1 is
    select OMCC.s_date, OMCC.s_hour, OMCC.lac_id, OMCC.cell_id, OMCC.lac_ci,
    W_DHL, W_DHL_BEF, W_DHL_NUM, W_DHL_NUM_BEF, W_DHL_D, W_DHL_D_BEF,
    W_JTL, W_JTL_BEF, W_JTL_N, W_JTL_N_BEF, W_JTL_D, W_JTL_D_BEF, W_JTL_QUEST, W_JTL_QUEST_BEF,
    W_RTWP, W_RTWP_BEF,
    W_GCJ, W_GCJ_BEF, W_GCJ_N, W_GCJ_N_BEF, W_CJ_NUM, W_CJ_NUM_BEF,
    W_RQH_CGL, W_RQH_CGL_BEF, W_RQH_CGL_N, W_RQH_CGL_N_BEF, W_RQH_QUEST, W_RQH_QUEST_BEF,
    W_HWL, W_HWL_BEF,
    province, city, country, factory, grid, company, loop_line, rnc,
    pre_field1, pre_field2, pre_field3, pre_field4, pre_field5, life
    FROM
    (
        select
        s_date, s_hour, lac_id, cell_id,
        lac_ci,
        100*decode(n3_0006, 0,null,null,null, round(n3_0005/n3_0006, 4) ) as W_DHL,
        round(n3_0005, 2) as W_DHL_NUM,
        round(n3_0006, 2) as W_DHL_D,

        100*decode(n3_0002*n3_0004, 0,null,null,null, round(n3_0001*n3_0003/(n3_0002*n3_0004), 4) ) as W_JTL,
        round(n3_0001*n3_0003, 2) as W_JTL_N,
        round(n3_0002*n3_0004, 2) as W_JTL_D,
        round(n3_0002, 2) as W_JTL_QUEST,

        decode(n3_0014, 0,null,null,null, round(n3_0014, 2) ) as W_RTWP,

        100*decode(n3_0008, 0,null,null,null, round(n3_0007/n3_0008, 4) ) as W_GCJ,
        round(n3_0007, 2) as W_GCJ_N,
        round(n3_0008, 2) as W_CJ_NUM,

        100*decode(n3_0016, 0,null,null,null, round(n3_0015/n3_0016, 4) ) as W_RQH_CGL,
        round(n3_0015, 2) as W_RQH_CGL_N,
        round(n3_0016, 2) as W_RQH_QUEST,

        round(n3_0009, 2) as W_HWL
        from
        (
            select s_date, s_hour, lac_id, cell_id,  lac_id||'-'||cell_id as lac_ci,
            n3_0001, n3_0002, n3_0003, n3_0004, n3_0005, n3_0006, n3_0007, n3_0008,
            n3_0014, n3_0015, n3_0016, n3_0009
            from omc_umts_3
            where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
            and s_hour between 9 and 23
            and s_hour = (case when V_DATE_HOUR is null then s_hour else cast(V_DATE_HOUR as int) end)
            -- partition(SYS_P202) t --205/14 202/15 204/21 203/22
        )--where lac_ci = '43061-23091'
    )OMCC
    left join
    (
        select
        s_date, s_hour, lac_id, cell_id,
        lac_ci,
        100*decode(n3_0006, 0,null,null,null, round(n3_0005/n3_0006, 4) ) as W_DHL_BEF,
        round(n3_0005, 2) as W_DHL_NUM_BEF,
        round(n3_0006, 2) as W_DHL_D_BEF,

        100*decode(n3_0002*n3_0004, 0,null,null,null, round(n3_0001*n3_0003/(n3_0002*n3_0004), 4) ) as W_JTL_BEF,
        round(n3_0001*n3_0003, 2) as W_JTL_N_BEF,
        round(n3_0002*n3_0004, 2) as W_JTL_D_BEF,
        round(n3_0002, 2) as W_JTL_QUEST_BEF,

        decode(n3_0014, 0,null,null,null, round(n3_0014, 2) ) as W_RTWP_BEF,

        100*decode(n3_0008, 0,null,null,null, round(n3_0007/n3_0008, 4) ) as W_GCJ_BEF,
        round(n3_0007, 2) as W_GCJ_N_BEF,
        round(n3_0008, 2) as W_CJ_NUM_BEF,

        100*decode(n3_0016, 0,null,null,null, round(n3_0015/n3_0016, 4) ) as W_RQH_CGL_BEF,
        round(n3_0015, 2) as W_RQH_CGL_N_BEF,
        round(n3_0016, 2) as W_RQH_QUEST_BEF,

        round(n3_0009, 2) as W_HWL_BEF
        from
        (
            select s_date, s_hour, lac_id, cell_id,  lac_id||'-'||cell_id as lac_ci,
            n3_0001, n3_0002, n3_0003, n3_0004, n3_0005, n3_0006, n3_0007, n3_0008,
            n3_0014, n3_0015, n3_0016, n3_0009
            from omc_umts_3
            where s_date >= to_date(V_DATE_THRESHOLD_START_7_AGO, 'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END_7_AGO, 'yyyymmdd')
            and s_hour between 9 and 23
            and s_hour = (case when V_DATE_HOUR is null then s_hour else cast(V_DATE_HOUR as int) end)
            -- partition(SYS_P202) t --205/14 202/15 204/21 203/22
        )--where lac_ci = '43061-23091'
    )OMCW
    on OMCC.s_hour= OMCW.s_hour and OMCC.lac_ci = OMCW.lac_ci
    left join
    (
        select lac||'-'||ci as lac_ci,
        t.* from DT_CELL_W t
    )DT
    on OMCC.lac_ci = DT.lac_ci;



    BEGIN
        V_TBNAME := 'TB_LIST_CELL_HOUR_W';

        --从系统中获取待插入分区名
        /*execute immediate 'select t.partition_name from USER_TAB_PARTITIONS t
        where t.table_name = :V_TBNAME
        and regexp_substr(t.partition_name,''[^_]+'',1,2,''i'') = :V_DATE_THRESHOLD_START; '
        into v_partition_name using V_TBNAME, V_DATE_THRESHOLD_START;*/
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);

        /*open cur_partition; --开始索引字典表
        fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        loop--------------
            exit when NOT (cur_partition%FOUND);
            v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
            if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    v_partition_name := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
            \*elsif (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START_7_AGO)
                then
                    v_partition_name_7_ago := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name_7_ago);*\
            end if;
            fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        end loop;
        close cur_partition;--------------*/


        if V_DATE_HOUR is not null
          then
              --小时级清理
              execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||') where s_hour='||V_DATE_HOUR into v_clean_flag;
              while v_clean_flag !=0 loop
                  --若入口参数指定了小时，则仅重跑指定小时对应的数据，此处索引至天级分区，并按小时清除
                  execute immediate 'DELETE FROM '||v_tbname||' PARTITION('||v_partition_name||') where s_hour='||V_DATE_HOUR;
                  commit;
                  dbms_output.put_line('表 '||v_tbname||' 的小时级数据已清理，清理时刻：'||V_DATE_HOUR);
                  /*select
                  'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                  into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                  where s.table_name = v_tbname;
                  execute immediate v_ssql;*/
                  --select count(1) into v_clean_flag from ZC_CELL_LIST_2G where s_date >= v_date_start and s_date < v_date_end;
                  execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||') where s_hour='||V_DATE_HOUR into v_clean_flag;
              end loop;
          elsif V_DATE_HOUR is null
          then
              --天级清理
              execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
              while v_clean_flag !=0 loop
                  select
                  'CALL PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                  into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                  where s.table_name = V_TBNAME;
                  execute immediate v_ssql;
                  execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
              end loop;
        end if;
        --准备工作完成！！

        OPEN CUR_SQL_1;

        LOOP
            FETCH cur_sql_1 BULK COLLECT INTO OMC_TAB LIMIT V_LIMIT; -- 使用limit子句限制提取数据量

            EXIT WHEN OMC_TAB.count = 0; -- 注意此时游标退出使用了 OMC_TAB.COUNT，而不是 OMC_TAB%notfound
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条

            FOR I IN OMC_TAB.FIRST .. OMC_TAB.LAST
            LOOP
                
                j := j + 1;
                --ZC
                if OMC_TAB(I).W_DHL_NUM > 30 and OMC_TAB(I).W_DHL > 5
                    then W_DHL_BADFLAG := 1; else W_DHL_BADFLAG := 0;
                end if;
                if OMC_TAB(I).W_RTWP > -95
                    then W_RTWP_BADFLAG := 1; else W_RTWP_BADFLAG := 0;
                end if;
                if OMC_TAB(I).W_CJ_NUM > 30 and OMC_TAB(I).W_GCJ > 5
                    then W_GCJ_BADFLAG := 1; else W_GCJ_BADFLAG := 0;
                end if;
                if OMC_TAB(I).W_JTL_QUEST > 100 and OMC_TAB(I).W_JTL < 90
                    then W_JTL_BADFLAG := 1; else W_JTL_BADFLAG := 0;
                end if;
                if OMC_TAB(I).W_RQH_QUEST > 100 and OMC_TAB(I).W_RQH_CGL < 80
                    then W_RQH_BADFLAG := 1; else W_RQH_BADFLAG := 0;
                end if;

                --TB
                if OMC_TAB(I).W_JTL_QUEST_BEF > 500 and OMC_TAB(I).W_JTL_BEF - OMC_TAB(I).W_JTL > 5
                    then W_JTL_TBFLAG := 1; else W_JTL_TBFLAG := 0;
                end if;

                if /*(OMC_TAB(I).W_DHL_NUM_BEF is not null and OMC_TAB(I).W_DHL_NUM_BEF <> 0 ) and*/ OMC_TAB(I).W_DHL_NUM_BEF > 10
                    then
                        if OMC_TAB(I).W_DHL_NUM/OMC_TAB(I).W_DHL_NUM_BEF >= 2
                            then W_DHL_TBFLAG := 1;
                        end if;
                else W_DHL_TBFLAG := 0;
                end if;

                if OMC_TAB(I).W_RTWP - OMC_TAB(I).W_RTWP_BEF > 15
                    then W_RTWP_TBFLAG := 1; else W_RTWP_TBFLAG := 0;
                end if;


                --dbms_output.put_line(OMC_TAB(I).s_hour||'-'||OMC_TAB(I).lac_ci||'-'||OMC_TAB(I).W_CJ_NUM_BEF);

                if /*(OMC_TAB(I).W_CJ_NUM_BEF is not null and OMC_TAB(I).W_CJ_NUM_BEF <> 0 ) and*/ OMC_TAB(I).W_CJ_NUM_BEF > 10
                    then
                        if OMC_TAB(I).W_CJ_NUM/OMC_TAB(I).W_CJ_NUM_BEF >= 2
                            then W_GCJ_TBFLAG := 1;
                        end if;
                else W_GCJ_TBFLAG := 0;
                end if;

                if OMC_TAB(I).W_RQH_QUEST_BEF > 100 and OMC_TAB(I).W_RQH_CGL_BEF - OMC_TAB(I).W_RQH_CGL > 5
                    then W_RQH_TBFLAG := 1; else W_RQH_TBFLAG := 0;
                end if;


                if /*(OMC_TAB(I).W_HWL_BEF is not null and OMC_TAB(I).W_HWL_BEF <> 0 ) and*/ OMC_TAB(I).W_HWL_BEF > 0.5
                    then
                        if (OMC_TAB(I).W_HWL_BEF - OMC_TAB(I).W_HWL)/OMC_TAB(I).W_HWL_BEF > 0.7
                            then W_HWL_TBFLAG := 1;
                        end if;
                else W_HWL_TBFLAG := 0;
                end if;

                --执行插入
                execute immediate 'insert /*+append*/ into TB_LIST_CELL_HOUR_W
                values(:S_DATE, :S_HOUR, :LAC_ID, :CELL_ID, :LAC_CI,
                :W_DHL, :W_DHL_BEF, :W_DHL_NUM, :W_DHL_NUM_BEF, :W_DHL_D, :W_DHL_D_BEF,
                :W_JTL, :W_JTL_BEF, :W_JTL_N, :W_JTL_N_BEF, :W_JTL_D, :W_JTL_D_BEF, :W_JTL_QUEST, :W_JTL_QUEST_BEF,
                :W_RTWP, :W_RTWP_BEF, :W_GCJ, :W_GCJ_BEF, :W_GCJ_N, :W_GCJ_N_BEF,
                :W_CJ_NUM, :W_CJ_NUM_BEF,
                :W_RQH_CGL, :W_RQH_CGL_BEF, :W_RQH_CGL_N, :W_RQH_CGL_N_BEF, :W_RQH_QUEST, :W_RQH_QUEST_BEF,
                :W_HWL, :W_HWL_BEF,
                :W_DHL_BADFLAG,
                :W_RTWP_BADFLAG,
                :W_GCJ_BADFLAG,
                :W_JTL_BADFLAG,
                :W_RQH_BADFLAG,
                :W_JTL_TBFLAG,
                :W_DHL_TBFLAG,
                :W_RTWP_TBFLAG,
                :W_GCJ_TBFLAG,
                :W_RQH_TBFLAG,
                :W_HWL_TBFLAG,
                :PROVINCE, :CITY, :COUNTRY, :FACTORY, :GRID,
                :COMPANY, :LOOP_LINE, :RNC,
                :PRE_FIELD1, :PRE_FIELD2, :PRE_FIELD3, :PRE_FIELD4, :PRE_FIELD5, :LIFE)'
                using
                OMC_TAB(I).S_DATE,
                OMC_TAB(I).S_HOUR,
                OMC_TAB(I).LAC_ID,
                OMC_TAB(I).CELL_ID,
                OMC_TAB(I).LAC_CI,
                OMC_TAB(I).W_DHL,
                OMC_TAB(I).W_DHL_BEF,
                OMC_TAB(I).W_DHL_NUM,
                OMC_TAB(I).W_DHL_NUM_BEF,
                OMC_TAB(I).W_DHL_D,
                OMC_TAB(I).W_DHL_D_BEF,
                OMC_TAB(I).W_JTL,
                OMC_TAB(I).W_JTL_BEF,
                OMC_TAB(I).W_JTL_N,
                OMC_TAB(I).W_JTL_N_BEF,
                OMC_TAB(I).W_JTL_D,
                OMC_TAB(I).W_JTL_D_BEF,
                OMC_TAB(I).W_JTL_QUEST,
                OMC_TAB(I).W_JTL_QUEST_BEF,
                OMC_TAB(I).W_RTWP,
                OMC_TAB(I).W_RTWP_BEF,
                OMC_TAB(I).W_GCJ,
                OMC_TAB(I).W_GCJ_BEF,
                OMC_TAB(I).W_GCJ_N,
                OMC_TAB(I).W_GCJ_N_BEF,
                OMC_TAB(I).W_CJ_NUM,
                OMC_TAB(I).W_CJ_NUM_BEF,
                OMC_TAB(I).W_RQH_CGL,
                OMC_TAB(I).W_RQH_CGL_BEF,
                OMC_TAB(I).W_RQH_CGL_N,
                OMC_TAB(I).W_RQH_CGL_N_BEF,
                OMC_TAB(I).W_RQH_QUEST,
                OMC_TAB(I).W_RQH_QUEST_BEF,
                OMC_TAB(I).W_HWL,
                OMC_TAB(I).W_HWL_BEF,

                W_DHL_BADFLAG,
                W_RTWP_BADFLAG,
                W_GCJ_BADFLAG,
                W_JTL_BADFLAG,
                W_RQH_BADFLAG,

                W_JTL_TBFLAG,
                W_DHL_TBFLAG,
                W_RTWP_TBFLAG,
                W_GCJ_TBFLAG,
                W_RQH_TBFLAG,
                W_HWL_TBFLAG,

                OMC_TAB(I).PROVINCE,
                OMC_TAB(I).CITY,
                OMC_TAB(I).COUNTRY,
                OMC_TAB(I).FACTORY,
                OMC_TAB(I).GRID,
                OMC_TAB(I).COMPANY,
                OMC_TAB(I).LOOP_LINE,
                OMC_TAB(I).RNC,
                OMC_TAB(I).PRE_FIELD1,
                OMC_TAB(I).PRE_FIELD2,
                OMC_TAB(I).PRE_FIELD3,
                OMC_TAB(I).PRE_FIELD4,
                OMC_TAB(I).PRE_FIELD5,
                OMC_TAB(I).LIFE;

                COMMIT;

            END LOOP;
        END LOOP;

        CLOSE CUR_SQL_1;

        dbms_output.put_line('j: '||j);

    END PROC_TB_LIST_CELL_HOUR_W;

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --in：TB_LIST_CELL_HOUR_W
    --out：TB_INDEX_CELL_DAY_W
    PROCEDURE PROC_TB_INDEX_CELL_DAY_W(V_DATE_THRESHOLD_START VARCHAR2) IS --小区小时指标
    --v_high_value_vc                 varchar2(20);
    v_partition_name                varchar2(50);
    --v_partition_name_7_ago          varchar2(50);
    --v_mr_partition_name                varchar2(50);
    --v_mr_partition_name_7_ago          varchar2(50);
    --V_DATE_THRESHOLD_START_7_AGO    varchar2(8);
    V_TBNAME                        varchar2(100);
    V_DATE_THRESHOLD_END    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd');--20190927

    -- V_DATE_THRESHOLD_START_7_AGO    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 7, 'yyyymmdd'); --2019/10/03 - 7 = 2019/09/26 → 20190926
    -- V_DATE_THRESHOLD_END_7_AGO      varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 6, 'yyyymmdd'); --20190927
    --v_s_date_7            date;
    sql_2                   varchar2(4000);

   /* v_insert_cnt   number;
    v_insert_repeat   number;*/
    v_ssql varchar2(500);
    v_clean_flag number;

    j number := 0;

    type LIST_3_TYPE is record
    (
        s_date          DATE,
        s_hour          INTEGER,
        lac_id          INTEGER,
        cell_id         INTEGER,
        lac_ci          VARCHAR2(50),
        w_dhl           NUMBER,
        w_dhl_bef       NUMBER,
        -- w_dhl_num       NUMBER,
        -- w_dhl_num_bef   NUMBER,
        -- w_dhl_d         NUMBER,
        -- w_dhl_d_bef     NUMBER,
        w_jtl           NUMBER,
        w_jtl_bef       NUMBER,
        -- w_jtl_n         NUMBER,
        -- w_jtl_n_bef     NUMBER,
        -- w_jtl_d         NUMBER,
        -- w_jtl_d_bef     NUMBER,
        -- w_jtl_quest     NUMBER,
        -- w_jtl_quest_bef NUMBER,
        w_rtwp          NUMBER,
        w_rtwp_bef      NUMBER,
        w_gcj           NUMBER,
        w_gcj_bef       NUMBER,
        -- w_gcj_n         NUMBER,
        -- w_gcj_n_bef     NUMBER,
        -- w_cj_num        NUMBER,
        -- w_cj_num_bef    NUMBER,
        w_rqh_cgl       NUMBER,
        w_rqh_cgl_bef   NUMBER,
        -- w_rqh_cgl_n     NUMBER,
        -- w_rqh_cgl_n_bef NUMBER,
        -- w_rqh_quest     NUMBER,
        -- w_rqh_quest_bef NUMBER,
        w_hwl           NUMBER,
        w_hwl_bef       NUMBER,
        w_dhl_badflag   VARCHAR2(5),
        w_rtwp_badflag  VARCHAR2(5),
        w_gcj_badflag   VARCHAR2(5),
        w_jtl_badflag   VARCHAR2(5),
        w_rqh_badflag   VARCHAR2(5),

        w_jtl_tbflag    VARCHAR2(5),
        w_dhl_tbflag    VARCHAR2(5),
        w_rtwp_tbflag   VARCHAR2(5),
        w_gcj_tbflag    VARCHAR2(5),
        w_rqh_tbflag    VARCHAR2(5),
        w_hwl_tbflag    VARCHAR2(5),

        province        VARCHAR2(32),
        city            VARCHAR2(32),
        country         VARCHAR2(64),
        factory         VARCHAR2(64),
        grid            VARCHAR2(64),
        company         VARCHAR2(64),
        loop_line       VARCHAR2(64),
        rnc             VARCHAR2(64),
        pre_field1      VARCHAR2(64),
        pre_field2      VARCHAR2(64),
        pre_field3      VARCHAR2(64),
        pre_field4      VARCHAR2(256),
        pre_field5      VARCHAR2(64),
        life            VARCHAR2(64)
    );

    OMC_TAB_OLD LIST_3_TYPE;

    -- 定义基于记录的嵌套表
    TYPE NESTED_OMC_TYPE IS TABLE OF LIST_3_TYPE;
    -- 声明集合变量
    OMC_TAB     NESTED_OMC_TYPE;

    --OMC_TAB_OLD  NESTED_OMC_TYPE := NESTED_OMC_TYPE();
    --OMC_TAB_NEW  NESTED_OMC_TYPE := NESTED_OMC_TYPE();


    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 600;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;

    m NUMBER := 0;
    n NUMBER := 0;

    --质差3临时判别
    W_DHL_BADFLAG_3_TEMP    VARCHAR2(5) := 0;
    W_RTWP_BADFLAG_3_TEMP   VARCHAR2(5) := 0;
    W_GCJ_BADFLAG_3_TEMP    VARCHAR2(5) := 0;
    W_JTL_BADFLAG_3_TEMP    VARCHAR2(5) := 0;
    W_RQH_BADFLAG_3_TEMP    VARCHAR2(5) := 0;

    --质差6临时判别
    W_DHL_BADFLAG_6_TEMP    VARCHAR2(5) := 0;
    W_RTWP_BADFLAG_6_TEMP   VARCHAR2(5) := 0;
    W_GCJ_BADFLAG_6_TEMP    VARCHAR2(5) := 0;
    W_JTL_BADFLAG_6_TEMP    VARCHAR2(5) := 0;
    W_RQH_BADFLAG_6_TEMP    VARCHAR2(5) := 0;

    --质差3结果
    W_DHL_BADFLAG_3         VARCHAR2(5) := 0;
    W_RTWP_BADFLAG_3        VARCHAR2(5) := 0;
    W_GCJ_BADFLAG_3         VARCHAR2(5) := 0;
    W_JTL_BADFLAG_3         VARCHAR2(5) := 0;
    W_RQH_BADFLAG_3         VARCHAR2(5) := 0;

    --质差6结果
    W_DHL_BADFLAG_6         VARCHAR2(5) := 0;
    W_RTWP_BADFLAG_6        VARCHAR2(5) := 0;
    W_GCJ_BADFLAG_6         VARCHAR2(5) := 0;
    W_JTL_BADFLAG_6         VARCHAR2(5) := 0;
    W_RQH_BADFLAG_6         VARCHAR2(5) := 0;

    --------------
    --突变3临时判别
    W_DHL_TBFLAG_3_TEMP     VARCHAR2(5) := 0;
    W_RTWP_TBFLAG_3_TEMP    VARCHAR2(5) := 0;
    W_GCJ_TBFLAG_3_TEMP     VARCHAR2(5) := 0;
    W_JTL_TBFLAG_3_TEMP     VARCHAR2(5) := 0;
    W_RQH_TBFLAG_3_TEMP     VARCHAR2(5) := 0;
    W_HWL_TBFLAG_3_TEMP     VARCHAR2(5) := 0;


    --突变6临时判别
    W_DHL_TBFLAG_6_TEMP     VARCHAR2(5) := 0;
    W_RTWP_TBFLAG_6_TEMP    VARCHAR2(5) := 0;
    W_GCJ_TBFLAG_6_TEMP     VARCHAR2(5) := 0;
    W_JTL_TBFLAG_6_TEMP     VARCHAR2(5) := 0;
    W_RQH_TBFLAG_6_TEMP     VARCHAR2(5) := 0;
    W_HWL_TBFLAG_6_TEMP     VARCHAR2(5) := 0;

    --突变3结果
    W_DHL_TBFLAG_3          VARCHAR2(5) := 0;
    W_RTWP_TBFLAG_3         VARCHAR2(5) := 0;
    W_GCJ_TBFLAG_3          VARCHAR2(5) := 0;
    W_JTL_TBFLAG_3          VARCHAR2(5) := 0;
    W_RQH_TBFLAG_3          VARCHAR2(5) := 0;
    W_HWL_TBFLAG_3          VARCHAR2(5) := 0;

    --突变6结果
    W_DHL_TBFLAG_6          VARCHAR2(5) := 0;
    W_RTWP_TBFLAG_6         VARCHAR2(5) := 0;
    W_GCJ_TBFLAG_6          VARCHAR2(5) := 0;
    W_JTL_TBFLAG_6          VARCHAR2(5) := 0;
    W_RQH_TBFLAG_6          VARCHAR2(5) := 0;
    W_HWL_TBFLAG_6          VARCHAR2(5) := 0;
    --------------

    --质差&突变终极标签
    W_DHL_BADFLAG           VARCHAR2(5) := 0;
    W_RTWP_BADFLAG          VARCHAR2(5) := 0;
    W_GCJ_BADFLAG           VARCHAR2(5) := 0;
    W_JTL_BADFLAG           VARCHAR2(5) := 0;
    W_RQH_BADFLAG           VARCHAR2(5) := 0;

    W_DHL_TBFLAG            VARCHAR2(5) := 0;
    W_RTWP_TBFLAG           VARCHAR2(5) := 0;
    W_GCJ_TBFLAG            VARCHAR2(5) := 0;
    W_JTL_TBFLAG            VARCHAR2(5) := 0;
    W_RQH_TBFLAG            VARCHAR2(5) := 0;
    W_HWL_TBFLAG            VARCHAR2(5) := 0;

    --质差&突变来源标签（来源分类：连续三小时或累计六小时）
    W_DHL_BADFLAG_SOURCE    VARCHAR2(5) := 0;
    W_RTWP_BADFLAG_SOURCE   VARCHAR2(5) := 0;
    W_GCJ_BADFLAG_SOURCE    VARCHAR2(5) := 0;
    W_JTL_BADFLAG_SOURCE    VARCHAR2(5) := 0;
    W_RQH_BADFLAG_SOURCE    VARCHAR2(5) := 0;

    W_DHL_TBFLAG_SOURCE     VARCHAR2(5) := 0;
    W_RTWP_TBFLAG_SOURCE    VARCHAR2(5) := 0;
    W_GCJ_TBFLAG_SOURCE     VARCHAR2(5) := 0;
    W_JTL_TBFLAG_SOURCE     VARCHAR2(5) := 0;
    W_RQH_TBFLAG_SOURCE     VARCHAR2(5) := 0;
    W_HWL_TBFLAG_SOURCE     VARCHAR2(5) := 0;

    /*type partition_type is record(
        table_name      varchar2(200),
        partition_name  varchar2(50),
        high_value      varchar2(100)
    );
    partition_tmp partition_type;

    cursor cur_partition is --字典表内获取非标准分区的分区名
    select table_name,t.partition_name,t.high_value
    from USER_TAB_PARTITIONS t
    where table_name = V_TBNAME and partition_name not like 'P2%'--SYS_21313
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历*/

    ------
    type curtyp is ref cursor;
    cur_sql_2 curtyp;

    type end_date_type is record(
        cur_sql_1_end_flag        VARCHAR2(50),
        cur_sql_1_hour_end_flag        INTEGER
    );
    end_flag end_date_type;
    ------


    cursor CUR_SQL_1 is
    select s_date, s_hour, lac_id, cell_id, lac_ci,
    w_dhl, w_dhl_bef, w_jtl, w_jtl_bef, w_rtwp, w_rtwp_bef, w_gcj, w_gcj_bef,
    w_rqh_cgl, w_rqh_cgl_bef, w_hwl, w_hwl_bef,
    w_dhl_badflag, w_rtwp_badflag, w_gcj_badflag, w_jtl_badflag, w_rqh_badflag,
    w_jtl_tbflag, w_dhl_tbflag, w_rtwp_tbflag, w_gcj_tbflag, w_rqh_tbflag, w_hwl_tbflag,
    province, city, country, factory, grid, company, loop_line, rnc,
    pre_field1, pre_field2, pre_field3, pre_field4, pre_field5, life --BULK COLLECT INTO OMC_TAB
    from TB_LIST_CELL_HOUR_W
    where s_date >= to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END, 'yyyymmdd')
    --and lac_ci >= '43054-11862' and lac_ci <=  '43054-11863'
    order by lac_ci, s_hour;

    BEGIN


        V_TBNAME := 'TB_INDEX_CELL_DAY_W';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);

        /*open cur_partition; --开始索引字典表
        fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        loop--------------
            exit when NOT (cur_partition%FOUND);
            v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
            if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    v_partition_name := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
            \*elsif (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START_7_AGO)
                then
                    v_partition_name_7_ago := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name_7_ago);*\
            end if;
            fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        end loop;
        close cur_partition;--------------*/


        --天级清理
        execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
        while v_clean_flag !=0 loop
            select
            'CALL PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
            into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
            where s.table_name = V_TBNAME;
            execute immediate v_ssql;
            execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
        end loop;
        --准备工作完成！！
        --索引分区名


        V_TBNAME := 'TB_LIST_CELL_HOUR_W';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);
        
        /*open cur_partition; --开始索引字典表
        fetch cur_partition into partition_tmp;
        loop--------------
            exit when NOT (cur_partition%FOUND);
            v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
            if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    v_partition_name := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name);
            -- elsif (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START_7_AGO)
            --     then
            --         v_partition_name_7_ago := partition_tmp.partition_name;
            --         dbms_output.put_line(v_partition_name_7_ago);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
            end if;
            fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        end loop;
        close cur_partition;--------------*/

        sql_2 :=
        'select max(lac_ci), max(s_hour) from TB_LIST_CELL_HOUR_W partition('||v_partition_name||') t1
        where lac_ci in
        (select max(lac_ci) lac_ci from TB_LIST_CELL_HOUR_W partition('||v_partition_name||') t
        )
        ';--where lac_ci =  ''43009-10251'' or lac_ci =  ''43009-10252''


        open cur_sql_2 for sql_2;
        fetch cur_sql_2 into end_flag;
        loop
            j := j + 1;
            dbms_output.put_line('本次检索的最大ECGI及其对应小时：'||end_flag.cur_sql_1_end_flag||'-'||end_flag.cur_sql_1_hour_end_flag);--当前时间戳对应的小区小时数据
            exit when (end_flag.cur_sql_1_end_flag is not null) or NOT cur_sql_2%FOUND or j >=1;
        end loop;
        close cur_sql_2;

        OPEN CUR_SQL_1;

        LOOP
            FETCH CUR_SQL_1 BULK COLLECT INTO OMC_TAB LIMIT V_LIMIT; -- 使用limit子句限制提取数据量

            EXIT WHEN OMC_TAB.count = 0; -- 注意此时游标退出使用了 OMC_TAB.COUNT，而不是 OMC_TAB%notfound
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条
            -- m := 0;



            FOR I IN OMC_TAB.FIRST .. OMC_TAB.LAST
            LOOP
                m := m + 1;
                n := n + 1;
                --OMC_TAB_NEW.extend;
                --OMC_TAB_OLD.extend;
                
                if m = 1 or OMC_TAB(I).LAC_CI != OMC_TAB_OLD.LAC_CI
                or (OMC_TAB_OLD.LAC_CI = end_flag.cur_sql_1_end_flag and OMC_TAB_OLD.S_HOUR = end_flag.cur_sql_1_hour_end_flag)
                    then
                        if m != 1 --非首次,则进该分支必是切换小区或达到遍历截止点，需进行最终判定并提交数据!!!
                            then
                                --ZC最终标签判定
                                --ZC-3
                                if W_DHL_BADFLAG_3_TEMP >= 2 then W_DHL_BADFLAG_3 := 1; end if;
                                if W_JTL_BADFLAG_3_TEMP >= 2 then W_JTL_BADFLAG_3 := 1; end if;
                                if W_RTWP_BADFLAG_3_TEMP >= 2 then W_RTWP_BADFLAG_3 := 1; end if;
                                if W_GCJ_BADFLAG_3_TEMP >= 2 then W_GCJ_BADFLAG_3 := 1; end if;
                                if W_RQH_BADFLAG_3_TEMP >= 2 then W_RQH_BADFLAG_3 := 1; end if;
                                --ZC-6
                                if W_DHL_BADFLAG_6_TEMP >= 6 then W_DHL_BADFLAG_6 := 1; end if;
                                if W_JTL_BADFLAG_6_TEMP >= 6 then W_JTL_BADFLAG_6 := 1; end if;
                                if W_RTWP_BADFLAG_6_TEMP >= 6 then W_RTWP_BADFLAG_6 := 1; end if;
                                if W_GCJ_BADFLAG_6_TEMP >= 6 then W_GCJ_BADFLAG_6 := 1; end if;
                                if W_RQH_BADFLAG_6_TEMP >= 6 then W_RQH_BADFLAG_6 := 1; end if;

                                --TB-3
                                if W_DHL_TBFLAG_3_TEMP >= 2 then W_DHL_TBFLAG_3 := 1; end if;
                                if W_JTL_TBFLAG_3_TEMP >= 2 then W_JTL_TBFLAG_3 := 1; end if;
                                if W_RTWP_TBFLAG_3_TEMP >= 2 then W_RTWP_TBFLAG_3 := 1; end if;
                                if W_GCJ_TBFLAG_3_TEMP >= 2 then W_GCJ_TBFLAG_3 := 1; end if;
                                if W_RQH_TBFLAG_3_TEMP >= 2 then W_RQH_TBFLAG_3 := 1; end if;
                                if W_HWL_TBFLAG_3_TEMP >= 2 then W_HWL_TBFLAG_3 := 1; end if;
                                --TB-6
                                if W_DHL_TBFLAG_6_TEMP >= 6 then W_DHL_TBFLAG_6 := 1; end if;
                                if W_JTL_TBFLAG_6_TEMP >= 6 then W_JTL_TBFLAG_6 := 1; end if;
                                if W_RTWP_TBFLAG_6_TEMP >= 6 then W_RTWP_TBFLAG_6 := 1; end if;
                                if W_GCJ_TBFLAG_6_TEMP >= 6 then W_GCJ_TBFLAG_6 := 1; end if;
                                if W_RQH_TBFLAG_6_TEMP >= 6 then W_RQH_TBFLAG_6 := 1; end if;
                                if W_HWL_TBFLAG_6_TEMP >= 6 then W_HWL_TBFLAG_6 := 1; end if;


                                --质差&突变终极标签、Source判定
                                --质差终极标签
                                if W_DHL_BADFLAG_3 = 1 and W_DHL_BADFLAG_6 = 1
                                    then
                                        W_DHL_BADFLAG := 1;
                                        W_DHL_BADFLAG_SOURCE := '3-6';
                                elsif W_DHL_BADFLAG_3 = 1 and W_DHL_BADFLAG_6 = 0
                                    then
                                        W_DHL_BADFLAG := 1;
                                        W_DHL_BADFLAG_SOURCE := '3';
                                elsif W_DHL_BADFLAG_3 = 0 and W_DHL_BADFLAG_6 = 1 --
                                    then
                                        W_DHL_BADFLAG := 1;
                                        W_DHL_BADFLAG_SOURCE := '6';
                                else
                                    W_DHL_BADFLAG := 0;
                                    W_DHL_BADFLAG_SOURCE := '0';
                                end if;

                                if W_JTL_BADFLAG_3 = 1 and W_JTL_BADFLAG_6 = 1
                                    then
                                        W_JTL_BADFLAG := 1;
                                        W_JTL_BADFLAG_SOURCE := '3-6';
                                elsif W_JTL_BADFLAG_3 = 1 and W_JTL_BADFLAG_6 = 0
                                    then
                                        W_JTL_BADFLAG := 1;
                                        W_JTL_BADFLAG_SOURCE := '3';
                                elsif W_JTL_BADFLAG_3 = 0 and W_JTL_BADFLAG_6 = 1 --
                                    then
                                        W_JTL_BADFLAG := 1;
                                        W_JTL_BADFLAG_SOURCE := '6';
                                else
                                    W_JTL_BADFLAG := 0;
                                    W_JTL_BADFLAG_SOURCE := '0';
                                end if;

                                if W_RTWP_BADFLAG_3 = 1 and W_RTWP_BADFLAG_6 = 1
                                    then
                                        W_RTWP_BADFLAG := 1;
                                        W_RTWP_BADFLAG_SOURCE := '3-6';
                                elsif W_RTWP_BADFLAG_3 = 1 and W_RTWP_BADFLAG_6 = 0
                                    then
                                        W_RTWP_BADFLAG := 1;
                                        W_RTWP_BADFLAG_SOURCE := '3';
                                elsif W_RTWP_BADFLAG_3 = 0 and W_RTWP_BADFLAG_6 = 1 --
                                    then
                                        W_RTWP_BADFLAG := 1;
                                        W_RTWP_BADFLAG_SOURCE := '6';
                                else
                                    W_RTWP_BADFLAG := 0;
                                    W_RTWP_BADFLAG_SOURCE := '0';
                                end if;

                                if W_GCJ_BADFLAG_3 = 1 and W_GCJ_BADFLAG_6 = 1
                                    then
                                        W_GCJ_BADFLAG := 1;
                                        W_GCJ_BADFLAG_SOURCE := '3-6';
                                elsif W_GCJ_BADFLAG_3 = 1 and W_GCJ_BADFLAG_6 = 0
                                    then
                                        W_GCJ_BADFLAG := 1;
                                        W_GCJ_BADFLAG_SOURCE := '3';
                                elsif W_GCJ_BADFLAG_3 = 0 and W_GCJ_BADFLAG_6 = 1 --
                                    then
                                        W_GCJ_BADFLAG := 1;
                                        W_GCJ_BADFLAG_SOURCE := '6';
                                else
                                    W_GCJ_BADFLAG := 0;
                                    W_GCJ_BADFLAG_SOURCE := '0';
                                end if;

                                if W_RQH_BADFLAG_3 = 1 and W_RQH_BADFLAG_6 = 1
                                    then
                                        W_RQH_BADFLAG := 1;
                                        W_RQH_BADFLAG_SOURCE := '3-6';
                                elsif W_RQH_BADFLAG_3 = 1 and W_RQH_BADFLAG_6 = 0
                                    then
                                        W_RQH_BADFLAG := 1;
                                        W_RQH_BADFLAG_SOURCE := '3';
                                elsif W_RQH_BADFLAG_3 = 0 and W_RQH_BADFLAG_6 = 1 --
                                    then
                                        W_RQH_BADFLAG := 1;
                                        W_RQH_BADFLAG_SOURCE := '6';
                                else
                                    W_RQH_BADFLAG := 0;
                                    W_RQH_BADFLAG_SOURCE := '0';
                                end if;


                                --突变终极标签
                                if W_DHL_TBFLAG_3 = 1 and W_DHL_TBFLAG_6 = 1
                                    then
                                        W_DHL_TBFLAG := 1;
                                        W_DHL_TBFLAG_SOURCE := '3-6';
                                elsif W_DHL_TBFLAG_3 = 1 and W_DHL_TBFLAG_6 = 0
                                    then
                                        W_DHL_TBFLAG := 1;
                                        W_DHL_TBFLAG_SOURCE := '3';
                                elsif W_DHL_TBFLAG_3 = 0 and W_DHL_TBFLAG_6 = 1 --
                                    then
                                        W_DHL_TBFLAG := 1;
                                        W_DHL_TBFLAG_SOURCE := '6';
                                else
                                    W_DHL_TBFLAG := 0;
                                    W_DHL_TBFLAG_SOURCE := '0';
                                end if;

                                if W_JTL_TBFLAG_3 = 1 and W_JTL_TBFLAG_6 = 1
                                    then
                                        W_JTL_TBFLAG := 1;
                                        W_JTL_TBFLAG_SOURCE := '3-6';
                                elsif W_JTL_TBFLAG_3 = 1 and W_JTL_TBFLAG_6 = 0
                                    then
                                        W_JTL_TBFLAG := 1;
                                        W_JTL_TBFLAG_SOURCE := '3';
                                elsif W_JTL_TBFLAG_3 = 0 and W_JTL_TBFLAG_6 = 1 --
                                    then
                                        W_JTL_TBFLAG := 1;
                                        W_JTL_TBFLAG_SOURCE := '6';
                                else
                                    W_JTL_TBFLAG := 0;
                                    W_JTL_TBFLAG_SOURCE := '0';
                                end if;

                                if W_RTWP_TBFLAG_3 = 1 and W_RTWP_TBFLAG_6 = 1
                                    then
                                        W_RTWP_TBFLAG := 1;
                                        W_RTWP_TBFLAG_SOURCE := '3-6';
                                elsif W_RTWP_TBFLAG_3 = 1 and W_RTWP_TBFLAG_6 = 0
                                    then
                                        W_RTWP_TBFLAG := 1;
                                        W_RTWP_TBFLAG_SOURCE := '3';
                                elsif W_RTWP_TBFLAG_3 = 0 and W_RTWP_TBFLAG_6 = 1 --
                                    then
                                        W_RTWP_TBFLAG := 1;
                                        W_RTWP_TBFLAG_SOURCE := '6';
                                else
                                    W_RTWP_TBFLAG := 0;
                                    W_RTWP_TBFLAG_SOURCE := '0';
                                end if;

                                if W_GCJ_TBFLAG_3 = 1 and W_GCJ_TBFLAG_6 = 1
                                    then
                                        W_GCJ_TBFLAG := 1;
                                        W_GCJ_TBFLAG_SOURCE := '3-6';
                                elsif W_GCJ_TBFLAG_3 = 1 and W_GCJ_TBFLAG_6 = 0
                                    then
                                        W_GCJ_TBFLAG := 1;
                                        W_GCJ_TBFLAG_SOURCE := '3';
                                elsif W_GCJ_TBFLAG_3 = 0 and W_GCJ_TBFLAG_6 = 1 --
                                    then
                                        W_GCJ_TBFLAG := 1;
                                        W_GCJ_TBFLAG_SOURCE := '6';
                                else
                                    W_GCJ_TBFLAG := 0;
                                    W_GCJ_TBFLAG_SOURCE := '0';
                                end if;

                                if W_RQH_TBFLAG_3 = 1 and W_RQH_TBFLAG_6 = 1
                                    then
                                        W_RQH_TBFLAG := 1;
                                        W_RQH_TBFLAG_SOURCE := '3-6';
                                elsif W_RQH_TBFLAG_3 = 1 and W_RQH_TBFLAG_6 = 0
                                    then
                                        W_RQH_TBFLAG := 1;
                                        W_RQH_TBFLAG_SOURCE := '3';
                                elsif W_RQH_TBFLAG_3 = 0 and W_RQH_TBFLAG_6 = 1 --
                                    then
                                        W_RQH_TBFLAG := 1;
                                        W_RQH_TBFLAG_SOURCE := '6';
                                else
                                    W_RQH_TBFLAG := 0;
                                    W_RQH_TBFLAG_SOURCE := '0';
                                end if;

                                if W_HWL_TBFLAG_3 = 1 and W_HWL_TBFLAG_6 = 1
                                    then
                                        W_HWL_TBFLAG := 1;
                                        W_HWL_TBFLAG_SOURCE := '3-6';
                                elsif W_HWL_TBFLAG_3 = 1 and W_HWL_TBFLAG_6 = 0
                                    then
                                        W_HWL_TBFLAG := 1;
                                        W_HWL_TBFLAG_SOURCE := '3';
                                elsif W_HWL_TBFLAG_3 = 0 and W_HWL_TBFLAG_6 = 1 --
                                    then
                                        W_HWL_TBFLAG := 1;
                                        W_HWL_TBFLAG_SOURCE := '6';
                                else
                                    W_HWL_TBFLAG := 0;
                                    W_HWL_TBFLAG_SOURCE := '0';
                                end if;

                                execute immediate 'insert /*+append*/ into TB_INDEX_CELL_DAY_W
                                values(
                                :s_date, :lac_id, :cell_id, :lac_ci,

                                :W_DHL_BADFLAG,
                                :W_RTWP_BADFLAG,
                                :W_GCJ_BADFLAG,
                                :W_JTL_BADFLAG,
                                :W_RQH_BADFLAG,

                                :W_DHL_TBFLAG,
                                :W_RTWP_TBFLAG,
                                :W_GCJ_TBFLAG,
                                :W_JTL_TBFLAG,
                                :W_RQH_TBFLAG,
                                :W_HWL_TBFLAG,

                                :W_DHL_BADFLAG_SOURCE,
                                :W_RTWP_BADFLAG_SOURCE,
                                :W_GCJ_BADFLAG_SOURCE,
                                :W_JTL_BADFLAG_SOURCE,
                                :W_RQH_BADFLAG_SOURCE,

                                :W_DHL_TBFLAG_SOURCE,
                                :W_RTWP_TBFLAG_SOURCE,
                                :W_GCJ_TBFLAG_SOURCE,
                                :W_JTL_TBFLAG_SOURCE,
                                :W_RQH_TBFLAG_SOURCE,
                                :W_HWL_TBFLAG_SOURCE,
                                :PROVINCE, :CITY, :COUNTRY, :FACTORY, :GRID,
                                :COMPANY, :LOOP_LINE, :RNC,
                                :PRE_FIELD1, :PRE_FIELD2, :PRE_FIELD3, :PRE_FIELD4, :PRE_FIELD5, :LIFE
                                )'
                                using
                                OMC_TAB_OLD.S_DATE,
                                OMC_TAB_OLD.LAC_ID, OMC_TAB_OLD.CELL_ID, OMC_TAB_OLD.LAC_CI,

                                W_DHL_BADFLAG,
                                W_RTWP_BADFLAG,
                                W_GCJ_BADFLAG,
                                W_JTL_BADFLAG,
                                W_RQH_BADFLAG,

                                W_DHL_TBFLAG,
                                W_RTWP_TBFLAG,
                                W_GCJ_TBFLAG,
                                W_JTL_TBFLAG,
                                W_RQH_TBFLAG,
                                W_HWL_TBFLAG,

                                W_DHL_BADFLAG_SOURCE,
                                W_RTWP_BADFLAG_SOURCE,
                                W_GCJ_BADFLAG_SOURCE,
                                W_JTL_BADFLAG_SOURCE,
                                W_RQH_BADFLAG_SOURCE,

                                W_DHL_TBFLAG_SOURCE,
                                W_RTWP_TBFLAG_SOURCE,
                                W_GCJ_TBFLAG_SOURCE,
                                W_JTL_TBFLAG_SOURCE,
                                W_RQH_TBFLAG_SOURCE,
                                W_HWL_TBFLAG_SOURCE,
                                OMC_TAB_OLD.PROVINCE, OMC_TAB_OLD.CITY, OMC_TAB_OLD.COUNTRY,
                                OMC_TAB_OLD.FACTORY, OMC_TAB_OLD.GRID,
                                OMC_TAB_OLD.COMPANY, OMC_TAB_OLD.LOOP_LINE, OMC_TAB_OLD.RNC,
                                OMC_TAB_OLD.PRE_FIELD1, OMC_TAB_OLD.PRE_FIELD2,
                                OMC_TAB_OLD.PRE_FIELD3, OMC_TAB_OLD.PRE_FIELD4,
                                OMC_TAB_OLD.PRE_FIELD5, OMC_TAB_OLD.LIFE;

                                if mod(n, 200)=0 --commit every 200 times
                                    then commit;
                                end if;

                        end if;


                        --判定初始化
                        --OMC_TAB_NEW(I) := OMC_TAB(I);
                        OMC_TAB_OLD := OMC_TAB(I);
                        m := 1;
                        --全局标签初始化
                        --质差3临时判别
                        W_DHL_BADFLAG_3_TEMP    := 0;
                        W_RTWP_BADFLAG_3_TEMP   := 0;
                        W_GCJ_BADFLAG_3_TEMP    := 0;
                        W_JTL_BADFLAG_3_TEMP    := 0;
                        W_RQH_BADFLAG_3_TEMP    := 0;
                        --质差6临时判别
                        W_DHL_BADFLAG_6_TEMP    := 0;
                        W_RTWP_BADFLAG_6_TEMP   := 0;
                        W_GCJ_BADFLAG_6_TEMP    := 0;
                        W_JTL_BADFLAG_6_TEMP    := 0;
                        W_RQH_BADFLAG_6_TEMP    := 0;
                        --质差3结果
                        W_DHL_BADFLAG_3         := 0;
                        W_RTWP_BADFLAG_3        := 0;
                        W_GCJ_BADFLAG_3         := 0;
                        W_JTL_BADFLAG_3         := 0;
                        W_RQH_BADFLAG_3         := 0;
                        --质差6结果
                        W_DHL_BADFLAG_6         := 0;
                        W_RTWP_BADFLAG_6        := 0;
                        W_GCJ_BADFLAG_6         := 0;
                        W_JTL_BADFLAG_6         := 0;
                        W_RQH_BADFLAG_6         := 0;

                        --------------
                        --突变3临时判别
                        W_DHL_TBFLAG_3_TEMP     := 0;
                        W_RTWP_TBFLAG_3_TEMP    := 0;
                        W_GCJ_TBFLAG_3_TEMP     := 0;
                        W_JTL_TBFLAG_3_TEMP     := 0;
                        W_RQH_TBFLAG_3_TEMP     := 0;
                        W_HWL_TBFLAG_3_TEMP     := 0;
                        --突变6临时判别
                        W_DHL_TBFLAG_6_TEMP     := 0;
                        W_RTWP_TBFLAG_6_TEMP    := 0;
                        W_GCJ_TBFLAG_6_TEMP     := 0;
                        W_JTL_TBFLAG_6_TEMP     := 0;
                        W_RQH_TBFLAG_6_TEMP     := 0;
                        W_HWL_TBFLAG_6_TEMP     := 0;
                        --突变3结果
                        W_DHL_TBFLAG_3          := 0;
                        W_RTWP_TBFLAG_3         := 0;
                        W_GCJ_TBFLAG_3          := 0;
                        W_JTL_TBFLAG_3          := 0;
                        W_RQH_TBFLAG_3          := 0;
                        W_HWL_TBFLAG_3          := 0;
                        --突变6结果
                        W_DHL_TBFLAG_6          := 0;
                        W_RTWP_TBFLAG_6         := 0;
                        W_GCJ_TBFLAG_6          := 0;
                        W_JTL_TBFLAG_6          := 0;
                        W_RQH_TBFLAG_6          := 0;
                        W_HWL_TBFLAG_6          := 0;
                        --------------

                        --质差&突变终极标签
                        W_DHL_BADFLAG           := 0;
                        W_RTWP_BADFLAG          := 0;
                        W_GCJ_BADFLAG           := 0;
                        W_JTL_BADFLAG           := 0;
                        W_RQH_BADFLAG           := 0;

                        W_DHL_TBFLAG            := 0;
                        W_RTWP_TBFLAG           := 0;
                        W_GCJ_TBFLAG            := 0;
                        W_JTL_TBFLAG            := 0;
                        W_RQH_TBFLAG            := 0;
                        W_HWL_TBFLAG            := 0;

                        --质差&突变来源标签（来源分类：连续三小时或累计六小时）
                        W_DHL_BADFLAG_SOURCE    := 0;
                        W_RTWP_BADFLAG_SOURCE   := 0;
                        W_GCJ_BADFLAG_SOURCE    := 0;
                        W_JTL_BADFLAG_SOURCE    := 0;
                        W_RQH_BADFLAG_SOURCE    := 0;

                        W_DHL_TBFLAG_SOURCE     := 0;
                        W_RTWP_TBFLAG_SOURCE    := 0;
                        W_GCJ_TBFLAG_SOURCE     := 0;
                        W_JTL_TBFLAG_SOURCE     := 0;
                        W_RQH_TBFLAG_SOURCE     := 0;
                        W_HWL_TBFLAG_SOURCE     := 0;
                        --continue;
                /*else
                    OMC_TAB_OLD := OMC_TAB(I);
                    --OMC_TAB_NEW(I) := OMC_TAB(I);*/
                end if;


                --ZC-3
                if  m != 1 and OMC_TAB_OLD.w_dhl_badflag = 1 and OMC_TAB(I).w_dhl_badflag = 1
                    then W_DHL_BADFLAG_3_TEMP := W_DHL_BADFLAG_3_TEMP + 1;
                end if;
                if m != 1 and OMC_TAB_OLD.w_jtl_badflag = 1 and OMC_TAB(I).w_jtl_badflag = 1
                    then W_JTL_BADFLAG_3_TEMP := W_JTL_BADFLAG_3_TEMP + 1;
                end if;
                if m != 1 and OMC_TAB_OLD.w_rtwp_badflag = 1 and OMC_TAB(I).w_rtwp_badflag = 1
                    then W_RTWP_BADFLAG_3_TEMP := W_RTWP_BADFLAG_3_TEMP + 1;
                end if;
                if m != 1 and OMC_TAB_OLD.w_gcj_badflag = 1 and OMC_TAB(I).w_gcj_badflag = 1
                    then W_GCJ_BADFLAG_3_TEMP := W_GCJ_BADFLAG_3_TEMP + 1;
                end if;
                if m != 1 and OMC_TAB_OLD.w_rqh_badflag = 1 and OMC_TAB(I).w_rqh_badflag = 1
                    then W_RQH_BADFLAG_3_TEMP := W_RQH_BADFLAG_3_TEMP + 1;
                end if;
                

                --ZC-6
                if OMC_TAB(I).w_dhl_badflag = 1 then W_DHL_BADFLAG_6_TEMP := W_DHL_BADFLAG_6_TEMP + 1; end if;
                if OMC_TAB(I).w_jtl_badflag = 1 then W_JTL_BADFLAG_6_TEMP := W_JTL_BADFLAG_6_TEMP + 1; end if;
                if OMC_TAB(I).w_rtwp_badflag = 1 then W_RTWP_BADFLAG_6_TEMP := W_RTWP_BADFLAG_6_TEMP + 1; end if;
                if OMC_TAB(I).w_gcj_badflag = 1 then W_GCJ_BADFLAG_6_TEMP := W_GCJ_BADFLAG_6_TEMP + 1; end if;
                if OMC_TAB(I).w_rqh_badflag = 1 then W_RQH_BADFLAG_6_TEMP := W_RQH_BADFLAG_6_TEMP + 1; end if;


                --TB-3
                if m != 1 and OMC_TAB_OLD.w_dhl_tbflag = 1 and OMC_TAB(I).w_dhl_tbflag = 1
                    then W_DHL_TBFLAG_3_TEMP := W_DHL_TBFLAG_3_TEMP + 1;
                end if;
                if m != 1 and OMC_TAB_OLD.w_jtl_tbflag = 1 and OMC_TAB(I).w_jtl_tbflag = 1
                    then W_JTL_TBFLAG_3_TEMP := W_JTL_TBFLAG_3_TEMP + 1;
                end if;
                if m != 1 and OMC_TAB_OLD.w_rtwp_tbflag = 1 and OMC_TAB(I).w_rtwp_tbflag = 1
                    then W_RTWP_TBFLAG_3_TEMP := W_RTWP_TBFLAG_3_TEMP + 1;
                end if;
                if m != 1 and OMC_TAB_OLD.w_gcj_tbflag = 1 and OMC_TAB(I).w_gcj_tbflag = 1
                    then W_GCJ_TBFLAG_3_TEMP := W_GCJ_TBFLAG_3_TEMP + 1;
                end if;
                if m != 1 and OMC_TAB_OLD.w_rqh_tbflag = 1 and OMC_TAB(I).w_rqh_tbflag = 1
                    then W_RQH_TBFLAG_3_TEMP := W_RQH_TBFLAG_3_TEMP + 1;
                end if;
                if m != 1 and OMC_TAB_OLD.w_hwl_tbflag = 1 and OMC_TAB(I).w_hwl_tbflag = 1
                    then W_HWL_TBFLAG_3_TEMP := W_HWL_TBFLAG_3_TEMP + 1;
                end if;

                --TB-6
                if OMC_TAB(I).w_dhl_tbflag = 1 THen W_DHL_TBFLAG_6_TEMP := W_DHL_TBFLAG_6_TEMP + 1; end if;
                if OMC_TAB(I).w_jtl_tbflag = 1 then W_JTL_TBFLAG_6_TEMP := W_JTL_TBFLAG_6_TEMP + 1; end if;
                if OMC_TAB(I).w_rtwp_tbflag = 1 then W_RTWP_TBFLAG_6_TEMP := W_RTWP_TBFLAG_6_TEMP + 1; end if;
                if OMC_TAB(I).w_gcj_tbflag = 1 then W_GCJ_TBFLAG_6_TEMP := W_GCJ_TBFLAG_6_TEMP + 1; end if;
                if OMC_TAB(I).w_rqh_tbflag = 1 then W_RQH_TBFLAG_6_TEMP := W_RQH_TBFLAG_6_TEMP + 1; end if;
                if OMC_TAB(I).w_hwl_tbflag = 1 then W_HWL_TBFLAG_6_TEMP := W_HWL_TBFLAG_6_TEMP + 1; end if;

                OMC_TAB_OLD := OMC_TAB(I);

                if OMC_TAB_OLD.LAC_CI = end_flag.cur_sql_1_end_flag and OMC_TAB_OLD.S_HOUR = end_flag.cur_sql_1_hour_end_flag
                    then
                        if W_DHL_BADFLAG_3_TEMP >= 2 then W_DHL_BADFLAG_3 := 1; end if;
                        if W_JTL_BADFLAG_3_TEMP >= 2 then W_JTL_BADFLAG_3 := 1; end if;
                        if W_RTWP_BADFLAG_3_TEMP >= 2 then W_RTWP_BADFLAG_3 := 1; end if;
                        if W_GCJ_BADFLAG_3_TEMP >= 2 then W_GCJ_BADFLAG_3 := 1; end if;
                        if W_RQH_BADFLAG_3_TEMP >= 2 then W_RQH_BADFLAG_3 := 1; end if;
                        --ZC-6
                        if W_DHL_BADFLAG_6_TEMP >= 6 then W_DHL_BADFLAG_6 := 1; end if;
                        if W_JTL_BADFLAG_6_TEMP >= 6 then W_JTL_BADFLAG_6 := 1; end if;
                        if W_RTWP_BADFLAG_6_TEMP >= 6 then W_RTWP_BADFLAG_6 := 1; end if;
                        if W_GCJ_BADFLAG_6_TEMP >= 6 then W_GCJ_BADFLAG_6 := 1; end if;
                        if W_RQH_BADFLAG_6_TEMP >= 6 then W_RQH_BADFLAG_6 := 1; end if;

                        --TB-3
                        if W_DHL_TBFLAG_3_TEMP >= 2 then W_DHL_TBFLAG_3 := 1; end if;
                        if W_JTL_TBFLAG_3_TEMP >= 2 then W_JTL_TBFLAG_3 := 1; end if;
                        if W_RTWP_TBFLAG_3_TEMP >= 2 then W_RTWP_TBFLAG_3 := 1; end if;
                        if W_GCJ_TBFLAG_3_TEMP >= 2 then W_GCJ_TBFLAG_3 := 1; end if;
                        if W_RQH_TBFLAG_3_TEMP >= 2 then W_RQH_TBFLAG_3 := 1; end if;
                        if W_HWL_TBFLAG_3_TEMP >= 2 then W_HWL_TBFLAG_3 := 1; end if;
                        --TB-6
                        if W_DHL_TBFLAG_6_TEMP >= 6 then W_DHL_TBFLAG_6 := 1; end if;
                        if W_JTL_TBFLAG_6_TEMP >= 6 then W_JTL_TBFLAG_6 := 1; end if;
                        if W_RTWP_TBFLAG_6_TEMP >= 6 then W_RTWP_TBFLAG_6 := 1; end if;
                        if W_GCJ_TBFLAG_6_TEMP >= 6 then W_GCJ_TBFLAG_6 := 1; end if;
                        if W_RQH_TBFLAG_6_TEMP >= 6 then W_RQH_TBFLAG_6 := 1; end if;
                        if W_HWL_TBFLAG_6_TEMP >= 6 then W_HWL_TBFLAG_6 := 1; end if;


                        --质差&突变终极标签、Source判定
                        --质差终极标签
                        if W_DHL_BADFLAG_3 = 1 and W_DHL_BADFLAG_6 = 1
                            then
                                W_DHL_BADFLAG := 1;
                                W_DHL_BADFLAG_SOURCE := '3-6';
                        elsif W_DHL_BADFLAG_3 = 1 and W_DHL_BADFLAG_6 = 0
                            then
                                W_DHL_BADFLAG := 1;
                                W_DHL_BADFLAG_SOURCE := '3';
                        elsif W_DHL_BADFLAG_3 = 0 and W_DHL_BADFLAG_6 = 1 --
                            then
                                W_DHL_BADFLAG := 1;
                                W_DHL_BADFLAG_SOURCE := '6';
                        else
                            W_DHL_BADFLAG := 0;
                            W_DHL_BADFLAG_SOURCE := '0';
                        end if;

                        if W_JTL_BADFLAG_3 = 1 and W_JTL_BADFLAG_6 = 1
                            then
                                W_JTL_BADFLAG := 1;
                                W_JTL_BADFLAG_SOURCE := '3-6';
                        elsif W_JTL_BADFLAG_3 = 1 and W_JTL_BADFLAG_6 = 0
                            then
                                W_JTL_BADFLAG := 1;
                                W_JTL_BADFLAG_SOURCE := '3';
                        elsif W_JTL_BADFLAG_3 = 0 and W_JTL_BADFLAG_6 = 1 --
                            then
                                W_JTL_BADFLAG := 1;
                                W_JTL_BADFLAG_SOURCE := '6';
                        else
                            W_JTL_BADFLAG := 0;
                            W_JTL_BADFLAG_SOURCE := '0';
                        end if;

                        if W_RTWP_BADFLAG_3 = 1 and W_RTWP_BADFLAG_6 = 1
                            then
                                W_RTWP_BADFLAG := 1;
                                W_RTWP_BADFLAG_SOURCE := '3-6';
                        elsif W_RTWP_BADFLAG_3 = 1 and W_RTWP_BADFLAG_6 = 0
                            then
                                W_RTWP_BADFLAG := 1;
                                W_RTWP_BADFLAG_SOURCE := '3';
                        elsif W_RTWP_BADFLAG_3 = 0 and W_RTWP_BADFLAG_6 = 1 --
                            then
                                W_RTWP_BADFLAG := 1;
                                W_RTWP_BADFLAG_SOURCE := '6';
                        else
                            W_RTWP_BADFLAG := 0;
                            W_RTWP_BADFLAG_SOURCE := '0';
                        end if;

                        if W_GCJ_BADFLAG_3 = 1 and W_GCJ_BADFLAG_6 = 1
                            then
                                W_GCJ_BADFLAG := 1;
                                W_GCJ_BADFLAG_SOURCE := '3-6';
                        elsif W_GCJ_BADFLAG_3 = 1 and W_GCJ_BADFLAG_6 = 0
                            then
                                W_GCJ_BADFLAG := 1;
                                W_GCJ_BADFLAG_SOURCE := '3';
                        elsif W_GCJ_BADFLAG_3 = 0 and W_GCJ_BADFLAG_6 = 1 --
                            then
                                W_GCJ_BADFLAG := 1;
                                W_GCJ_BADFLAG_SOURCE := '6';
                        else
                            W_GCJ_BADFLAG := 0;
                            W_GCJ_BADFLAG_SOURCE := '0';
                        end if;

                        if W_RQH_BADFLAG_3 = 1 and W_RQH_BADFLAG_6 = 1
                            then
                                W_RQH_BADFLAG := 1;
                                W_RQH_BADFLAG_SOURCE := '3-6';
                        elsif W_RQH_BADFLAG_3 = 1 and W_RQH_BADFLAG_6 = 0
                            then
                                W_RQH_BADFLAG := 1;
                                W_RQH_BADFLAG_SOURCE := '3';
                        elsif W_RQH_BADFLAG_3 = 0 and W_RQH_BADFLAG_6 = 1 --
                            then
                                W_RQH_BADFLAG := 1;
                                W_RQH_BADFLAG_SOURCE := '6';
                        else
                            W_RQH_BADFLAG := 0;
                            W_RQH_BADFLAG_SOURCE := '0';
                        end if;


                        --突变终极标签
                        if W_DHL_TBFLAG_3 = 1 and W_DHL_TBFLAG_6 = 1
                            then
                                W_DHL_TBFLAG := 1;
                                W_DHL_TBFLAG_SOURCE := '3-6';
                        elsif W_DHL_TBFLAG_3 = 1 and W_DHL_TBFLAG_6 = 0
                            then
                                W_DHL_TBFLAG := 1;
                                W_DHL_TBFLAG_SOURCE := '3';
                        elsif W_DHL_TBFLAG_3 = 0 and W_DHL_TBFLAG_6 = 1 --
                            then
                                W_DHL_TBFLAG := 1;
                                W_DHL_TBFLAG_SOURCE := '6';
                        else
                            W_DHL_TBFLAG := 0;
                            W_DHL_TBFLAG_SOURCE := '0';
                        end if;

                        if W_JTL_TBFLAG_3 = 1 and W_JTL_TBFLAG_6 = 1
                            then
                                W_JTL_TBFLAG := 1;
                                W_JTL_TBFLAG_SOURCE := '3-6';
                        elsif W_JTL_TBFLAG_3 = 1 and W_JTL_TBFLAG_6 = 0
                            then
                                W_JTL_TBFLAG := 1;
                                W_JTL_TBFLAG_SOURCE := '3';
                        elsif W_JTL_TBFLAG_3 = 0 and W_JTL_TBFLAG_6 = 1 --
                            then
                                W_JTL_TBFLAG := 1;
                                W_JTL_TBFLAG_SOURCE := '6';
                        else
                            W_JTL_TBFLAG := 0;
                            W_JTL_TBFLAG_SOURCE := '0';
                        end if;

                        if W_RTWP_TBFLAG_3 = 1 and W_RTWP_TBFLAG_6 = 1
                            then
                                W_RTWP_TBFLAG := 1;
                                W_RTWP_TBFLAG_SOURCE := '3-6';
                        elsif W_RTWP_TBFLAG_3 = 1 and W_RTWP_TBFLAG_6 = 0
                            then
                                W_RTWP_TBFLAG := 1;
                                W_RTWP_TBFLAG_SOURCE := '3';
                        elsif W_RTWP_TBFLAG_3 = 0 and W_RTWP_TBFLAG_6 = 1 --
                            then
                                W_RTWP_TBFLAG := 1;
                                W_RTWP_TBFLAG_SOURCE := '6';
                        else
                            W_RTWP_TBFLAG := 0;
                            W_RTWP_TBFLAG_SOURCE := '0';
                        end if;

                        if W_GCJ_TBFLAG_3 = 1 and W_GCJ_TBFLAG_6 = 1
                            then
                                W_GCJ_TBFLAG := 1;
                                W_GCJ_TBFLAG_SOURCE := '3-6';
                        elsif W_GCJ_TBFLAG_3 = 1 and W_GCJ_TBFLAG_6 = 0
                            then
                                W_GCJ_TBFLAG := 1;
                                W_GCJ_TBFLAG_SOURCE := '3';
                        elsif W_GCJ_TBFLAG_3 = 0 and W_GCJ_TBFLAG_6 = 1 --
                            then
                                W_GCJ_TBFLAG := 1;
                                W_GCJ_TBFLAG_SOURCE := '6';
                        else
                            W_GCJ_TBFLAG := 0;
                            W_GCJ_TBFLAG_SOURCE := '0';
                        end if;

                        if W_RQH_TBFLAG_3 = 1 and W_RQH_TBFLAG_6 = 1
                            then
                                W_RQH_TBFLAG := 1;
                                W_RQH_TBFLAG_SOURCE := '3-6';
                        elsif W_RQH_TBFLAG_3 = 1 and W_RQH_TBFLAG_6 = 0
                            then
                                W_RQH_TBFLAG := 1;
                                W_RQH_TBFLAG_SOURCE := '3';
                        elsif W_RQH_TBFLAG_3 = 0 and W_RQH_TBFLAG_6 = 1 --
                            then
                                W_RQH_TBFLAG := 1;
                                W_RQH_TBFLAG_SOURCE := '6';
                        else
                            W_RQH_TBFLAG := 0;
                            W_RQH_TBFLAG_SOURCE := '0';
                        end if;

                        if W_HWL_TBFLAG_3 = 1 and W_HWL_TBFLAG_6 = 1
                            then
                                W_HWL_TBFLAG := 1;
                                W_HWL_TBFLAG_SOURCE := '3-6';
                        elsif W_HWL_TBFLAG_3 = 1 and W_HWL_TBFLAG_6 = 0
                            then
                                W_HWL_TBFLAG := 1;
                                W_HWL_TBFLAG_SOURCE := '3';
                        elsif W_HWL_TBFLAG_3 = 0 and W_HWL_TBFLAG_6 = 1 --
                            then
                                W_HWL_TBFLAG := 1;
                                W_HWL_TBFLAG_SOURCE := '6';
                        else
                            W_HWL_TBFLAG := 0;
                            W_HWL_TBFLAG_SOURCE := '0';
                        end if;

                        execute immediate 'insert /*+append*/ into TB_INDEX_CELL_DAY_W
                        values(
                        :s_date, :lac_id, :cell_id, :lac_ci,

                        :W_DHL_BADFLAG,
                        :W_RTWP_BADFLAG,
                        :W_GCJ_BADFLAG,
                        :W_JTL_BADFLAG,
                        :W_RQH_BADFLAG,

                        :W_DHL_TBFLAG,
                        :W_RTWP_TBFLAG,
                        :W_GCJ_TBFLAG,
                        :W_JTL_TBFLAG,
                        :W_RQH_TBFLAG,
                        :W_HWL_TBFLAG,

                        :W_DHL_BADFLAG_SOURCE,
                        :W_RTWP_BADFLAG_SOURCE,
                        :W_GCJ_BADFLAG_SOURCE,
                        :W_JTL_BADFLAG_SOURCE,
                        :W_RQH_BADFLAG_SOURCE,

                        :W_DHL_TBFLAG_SOURCE,
                        :W_RTWP_TBFLAG_SOURCE,
                        :W_GCJ_TBFLAG_SOURCE,
                        :W_JTL_TBFLAG_SOURCE,
                        :W_RQH_TBFLAG_SOURCE,
                        :W_HWL_TBFLAG_SOURCE,
                        :PROVINCE, :CITY, :COUNTRY, :FACTORY, :GRID,
                        :COMPANY, :LOOP_LINE, :RNC,
                        :PRE_FIELD1, :PRE_FIELD2, :PRE_FIELD3, :PRE_FIELD4, :PRE_FIELD5, :LIFE
                        )'
                        using
                        OMC_TAB_OLD.S_DATE,
                        OMC_TAB_OLD.LAC_ID, OMC_TAB_OLD.CELL_ID, OMC_TAB_OLD.LAC_CI,

                        W_DHL_BADFLAG,
                        W_RTWP_BADFLAG,
                        W_GCJ_BADFLAG,
                        W_JTL_BADFLAG,
                        W_RQH_BADFLAG,

                        W_DHL_TBFLAG,
                        W_RTWP_TBFLAG,
                        W_GCJ_TBFLAG,
                        W_JTL_TBFLAG,
                        W_RQH_TBFLAG,
                        W_HWL_TBFLAG,

                        W_DHL_BADFLAG_SOURCE,
                        W_RTWP_BADFLAG_SOURCE,
                        W_GCJ_BADFLAG_SOURCE,
                        W_JTL_BADFLAG_SOURCE,
                        W_RQH_BADFLAG_SOURCE,

                        W_DHL_TBFLAG_SOURCE,
                        W_RTWP_TBFLAG_SOURCE,
                        W_GCJ_TBFLAG_SOURCE,
                        W_JTL_TBFLAG_SOURCE,
                        W_RQH_TBFLAG_SOURCE,
                        W_HWL_TBFLAG_SOURCE,
                        OMC_TAB_OLD.PROVINCE, OMC_TAB_OLD.CITY, OMC_TAB_OLD.COUNTRY,
                        OMC_TAB_OLD.FACTORY, OMC_TAB_OLD.GRID,
                        OMC_TAB_OLD.COMPANY, OMC_TAB_OLD.LOOP_LINE, OMC_TAB_OLD.RNC,
                        OMC_TAB_OLD.PRE_FIELD1, OMC_TAB_OLD.PRE_FIELD2,
                        OMC_TAB_OLD.PRE_FIELD3, OMC_TAB_OLD.PRE_FIELD4,
                        OMC_TAB_OLD.PRE_FIELD5, OMC_TAB_OLD.LIFE;

                        if mod(n, 200)=0 --commit every 200 times
                            then commit;
                        end if;
                end if;


            END LOOP;

            COMMIT;
        END LOOP;

        CLOSE CUR_SQL_1;


        dbms_output.put_line('n: '||n||' V_COUNTER: '||V_COUNTER);

    END PROC_TB_INDEX_CELL_DAY_W;


-----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
    --in：TB_INDEX_CELL_DAY_W / DT_CELL_W
    --out：OSS_CELL_NUM
    PROCEDURE PROC_TB_OSS_DAY_W(V_DATE_THRESHOLD_START VARCHAR2) IS
    V_TBNAME                    varchar2(100);
    --V_DATE_THRESHOLD_END            varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd'); --20191004
    v_partition_name_clean      varchar2(50);
    v_partition_name            varchar2(50);
    v_high_value_vc             varchar2(20);
    
    v_timeout           date;
    v_insert_cnt        number;
    v_insert_repeat     number;
    --v_ssql              varchar2(500);
    v_clean_flag        number;
    type OSS_TYPE is record
    (
        s_date       DATE,
        area         VARCHAR2(10),
        area_level   VARCHAR2(20),
        problem_type VARCHAR2(20),
        net_type     VARCHAR2(10),
        bad_cell_num NUMBER,
        omc_cell_num NUMBER
    );
    -- 定义基于记录的嵌套表
    TYPE NESTED_OSS_TYPE IS TABLE OF OSS_TYPE;
    -- 声明集合变量
    OSS_TAB          NESTED_OSS_TYPE;

    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 500;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;    
    
    type partition_type is record
    (
        table_name      varchar2(200),
        partition_name  varchar2(50),
        high_value      varchar2(100)
    );
    partition_tmp partition_type;

    cursor cur_partition is --字典表内获取非标准分区的分区名
    select table_name,t.partition_name,t.high_value
    from USER_TAB_PARTITIONS t
    where table_name = V_TBNAME and partition_name not like 'P2%'--SYS_21313
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历
    
    --cursor cur_sql_2 is
    SQL_1 VARCHAR2(4000);
    SQL_2 VARCHAR2(4000);
    /*SQL_1 clob;
    SQL_2 clob;*/
    TYPE CurTyp IS REF CURSOR;
    
    CUR_SQL_1	CurTyp;
    
    j number := 0;

    BEGIN
       
       --程序超时判定的起始时间设置
        v_timeout := sysdate;
        
        V_TBNAME := 'OSS_CELL_NUM';
        --索引分区名
        open cur_partition; --开始索引字典表
        fetch cur_partition into partition_tmp;
        loop--------------
            exit when NOT (cur_partition%FOUND);
            v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
            if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    v_partition_name_clean := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name_clean);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
            end if;
            fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        end loop;
        close cur_partition;--------------
        
        --清理
        if v_partition_name_clean is not null
            then
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name_clean||') where net_type = ''3G''' into v_clean_flag;
                while v_clean_flag !=0 
                loop
                    execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_partition_name_clean||') where net_type = ''3G''';
                    commit;
                    execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name_clean||') where net_type = ''3G''' into v_clean_flag;
                end loop;
        end if;  
        
        V_TBNAME := 'TB_INDEX_CELL_DAY';
        open cur_partition; --开始索引字典表
        fetch cur_partition into partition_tmp;
        loop--------------
            exit when NOT (cur_partition%FOUND);
            v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
            if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    v_partition_name := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
            end if;
            fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        end loop;
        close cur_partition;--------------
        --准备工作完成！！


        --SQL_2：全网
        SQL_2 := 
        'select s_date, t1.area, t1.area_level, problem_type, ''3G'' net_type, t2.cell_sum- good_cell_num as good_cell_num, t2.cell_sum from
        (
            select s_date, ''优化分区'' as area, country as area_level, ''质差-3G接通率'' problem_type, count(W_JTL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_JTL_BADFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''质差-3G掉话率'' problem_type, count(W_DHL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_DHL_BADFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''质差-3G高干扰'' problem_type,  count(W_RTWP_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_RTWP_BADFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''质差-3G高重建'' problem_type,  count(W_GCJ_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_GCJ_BADFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''质差-3G软切换成功率'' problem_type,  count(W_RQH_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_RQH_BADFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''突变-3G接通率'' problem_type,  count(W_JTL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_JTL_TBFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''突变-3G掉话率'' problem_type,  count(W_DHL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_DHL_TBFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''突变-3G高干扰'' problem_type,  count(W_RTWP_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_RTWP_TBFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''突变-3G高重建'' problem_type,  count(W_GCJ_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_GCJ_TBFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''突变-3G软切换成功率'' problem_type,  count(W_RQH_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_RQH_TBFLAG = 0 group by s_date, country
            union all
            select s_date, ''优化分区'' as area, country as area_level, ''突变-3G话务量'' problem_type,  count(W_HWL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_HWL_TBFLAG = 0 group by s_date, country
        )t1,
        (
            select tb.country as area_level_dt, count(tb.lac_ci) cell_sum
            from
            (
                select distinct lac_ci, country from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t
            )tb,
            (
                select lac||''-''||ci lac_ci_dt, country from DT_CELL_W
            )dt
            where tb.lac_ci = dt.lac_ci_dt and tb.country is not null
            group by tb.country
        )t2
        where t1.area_level = t2.area_level_dt';
        --dbms_output.put_line(SQL_2);

        --SQL_1：全网
        SQL_1 := '
        select s_date, t1.area, t1.area_level, problem_type,  ''3G'' net_type, /*good_cell_num,*/ t2.cell_sum- good_cell_num as good_cell_num, t2.cell_sum from
        (
            select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-3G接通率'' problem_type,  count(W_JTL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_JTL_BADFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-3G掉话率'' problem_type,  count(W_DHL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_DHL_BADFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-3G高干扰'' problem_type,  count(W_RTWP_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_RTWP_BADFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-3G高重建'' problem_type,  count(W_GCJ_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_GCJ_BADFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-3G软切换成功率'' problem_type,  count(W_RQH_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_RQH_BADFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-3G接通率'' problem_type,  count(W_JTL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_JTL_TBFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-3G掉话率'' problem_type,  count(W_DHL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_DHL_TBFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-3G高干扰'' problem_type,  count(W_RTWP_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_RTWP_TBFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-3G高重建'' problem_type,  count(W_GCJ_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_GCJ_TBFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-3G软切换成功率'' problem_type,  count(W_RQH_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_RQH_TBFLAG = 0 group by s_date
            union all
            select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-3G话务量'' problem_type,  count(W_HWL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY_W partition('||v_partition_name||') t where W_HWL_TBFLAG = 0 group by s_date
        )t1,
        (
            select ''上海'' as area, ''上海'' as area_level, count(distinct lac_ci) cell_sum from TB_INDEX_CELL_DAY_W partition('||v_partition_name||')
        )t2
        where t1.area_level = t2.area_level';
        --dbms_output.put_line(SQL_1);
        
        
        --分区统计结果load
        OPEN CUR_SQL_1 FOR SQL_1;
        LOOP
            FETCH CUR_SQL_1 BULK COLLECT INTO OSS_TAB LIMIT V_LIMIT;
            EXIT WHEN OSS_TAB.count = 0 or round(to_number(sysdate - v_timeout) * 24 * 60) >= 10;
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条


            FOR I IN OSS_TAB.FIRST .. OSS_TAB.LAST
            LOOP
            j := j + 1;

            --执行插入
            execute immediate 'insert /*+append*/ into OSS_CELL_NUM
            values(
            :s_date, 
            :area, 
            :area_level, 
            :problem_type, 
            :net_type, 
            :bad_cell_num, 
            :omc_cell_num
            )'
            using
            OSS_TAB(I).s_date, 
            OSS_TAB(I).area, 
            OSS_TAB(I).area_level, 
            OSS_TAB(I).problem_type, 
            OSS_TAB(I).net_type, 
            OSS_TAB(I).bad_cell_num, 
            OSS_TAB(I).omc_cell_num
            ;
            if mod(j, 100)=0 --commit every 100 times
                then commit;
            end if;
            
            --超时判定
            if round(to_number(sysdate - v_timeout) * 24 * 60) >= 13 then return;
            end if;
            
            END LOOP;
        END LOOP;
        COMMIT;
        CLOSE CUR_SQL_1;
        
        v_timeout := sysdate;
        
        --全网统计结果load
        OPEN CUR_SQL_1 FOR SQL_2;
        LOOP
            FETCH CUR_SQL_1 BULK COLLECT INTO OSS_TAB LIMIT V_LIMIT;
            EXIT WHEN OSS_TAB.count = 0 or round(to_number(sysdate - v_timeout) * 24 * 60) >= 10;
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条


            FOR I IN OSS_TAB.FIRST .. OSS_TAB.LAST
            LOOP
            j := j + 1;

            --执行插入
            execute immediate 'insert /*+append*/ into OSS_CELL_NUM
            values(
            :s_date, 
            :area, 
            :area_level, 
            :problem_type, 
            :net_type, 
            :bad_cell_num, 
            :omc_cell_num
            )'
            using
            OSS_TAB(I).s_date, 
            OSS_TAB(I).area, 
            OSS_TAB(I).area_level, 
            OSS_TAB(I).problem_type, 
            OSS_TAB(I).net_type, 
            OSS_TAB(I).bad_cell_num, 
            OSS_TAB(I).omc_cell_num
            ;
            if mod(j, 100)=0 --commit every 100 times
                then commit;
            end if;
            
            --超时判定
            if round(to_number(sysdate - v_timeout) * 24 * 60) >= 13 then return;
            end if;
            
            END LOOP;
        END LOOP;
        COMMIT;
        CLOSE CUR_SQL_1;

        dbms_output.put_line('j: '||j);

        --若是首次插入，这里会去遍历分区名，不然到最后一步统计会出现分区不存在的问题
        V_TBNAME := 'OSS_CELL_NUM';
        
        open cur_partition; --开始索引字典表
        fetch cur_partition into partition_tmp;
        loop--------------
            exit when NOT (cur_partition%FOUND);
            v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
            if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    v_partition_name_clean := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name_clean);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
            end if;
            fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        end loop;
        close cur_partition;--------------
        
        --入库数量判断
        execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name_clean||') where net_type = ''3G''' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name_clean||') 
        where net_type = ''3G'' group by s_date, area, area_level, problem_type having count(1)>1)'into v_insert_repeat;
        dbms_output.put_line('表 '||v_tbname||' 天级数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');
        
    
    END PROC_TB_OSS_DAY_W;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --in：TB_LIST_CELL_HOUR_W
    --out：TB_EXPORT_TMP_W
    PROCEDURE PROC_TB_LIST_CELL_HOUR_FORMAT_W(V_DATE_THRESHOLD_START VARCHAR2) IS --小区小时指标临时表
    -- v_high_value_vc                 varchar2(20);
    -- v_partition_name                varchar2(50);
    -- v_partition_name_7_ago          varchar2(50);
    /* v_mr_partition_name             varchar2(50);
    v_mr_partition_name_7_ago          varchar2(50); */
    V_TBNAME                            varchar2(100);
    V_DATE_THRESHOLD_END                varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd'); --20191004

    --V_DATE_THRESHOLD_START_7_AGO    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 7, 'yyyymmdd'); --2019/10/03 - 7 = 2019/09/26 → 20190926
    --V_DATE_THRESHOLD_END_7_AGO      varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 6, 'yyyymmdd'); --20190927
    --sql_2                           varchar2(4000);

    v_insert_cnt   number;
    --v_insert_repeat   number;
    --v_ssql varchar2(500);
    --v_clean_flag number;

    j number := 0;

    type tbzc_export_3_type is record
    (
        s_date          DATE,
        s_hour          INTEGER,
        lac_id          INTEGER,
        cell_id         INTEGER,
        lac_ci          VARCHAR2(50),
        w_jtl           NUMBER,
        w_jtl_bef       NUMBER,
        w_dhl           NUMBER,
        w_dhl_bef       NUMBER,
        w_rtwp          NUMBER,
        w_rtwp_bef      NUMBER,
        w_gcj           NUMBER,
        w_gcj_bef       NUMBER,
        w_rqh_cgl       NUMBER,
        w_rqh_cgl_bef   NUMBER,
        w_hwl           NUMBER,
        w_hwl_bef       NUMBER,
        w_dhl_num       NUMBER,
        w_dhl_num_bef   NUMBER,
        w_cj_num        NUMBER,
        w_cj_num_bef    NUMBER
    );
    --list_tmp_1 TBZC_EXPORT_3_TYPE;

    -- 定义基于记录的嵌套表
    TYPE NESTED_EXPORT_TYPE IS TABLE OF TBZC_EXPORT_3_TYPE;
    -- 声明集合变量
    EXPORT_TAB          NESTED_EXPORT_TYPE;

    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 1000;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;


    cursor CUR_SQL_1 is
    select t1.s_date, t1.s_hour, t1.LAC_ID, t1.cell_id, t1.LAC_CI,
    /*jtl_badflag, dxl_badflag, avg_rtwp_badflag, csfb_cgl_badflag, tpqh_cgl_badflag, mr_badflag,
    jtl_tbflag, dxl_tbflag, avg_rtwp_tbflag, ll_tbflag, csfb_cgl_tbflag, tpqh_cgl_tbflag, ta_tbflag, mr_tbflag,*/
    w_jtl, w_jtl_bef, w_dhl, w_dhl_bef, w_rtwp, w_rtwp_bef, w_gcj, w_gcj_bef,
    w_rqh_cgl, w_rqh_cgl_bef, w_hwl, w_hwl_bef,
    t1.w_dhl_num, t1.w_dhl_num_bef, t1.w_cj_num, t1.w_cj_num_bef

    /* province, city, country, factory, grid, company, loop_line, rnc,
    pre_field1, pre_field2, pre_field3, pre_field4, pre_field5, life */
    from TB_LIST_CELL_HOUR_W t1
    --where t.s_date = to_date(20191003, 'yyyymmdd') --and t.ecgi = '8397606'
    where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd');


    --多行转多列
    --详见 PROC_TB_LIST_CELL_HOUR_FORMAT2

    BEGIN
        V_TBNAME := 'TB_EXPORT_TMP_W';

        --索引分区名
        -- open cur_partition; --开始索引字典表
        -- fetch cur_partition into partition_tmp;
        -- loop--------------
        --     exit when NOT (cur_partition%FOUND);
        --     v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
        --     if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
        --         then
        --             v_partition_name := partition_tmp.partition_name;
        --             dbms_output.put_line(v_partition_name);
        --     elsif (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START_7_AGO)
        --         then
        --             v_partition_name_7_ago := partition_tmp.partition_name;
        --             dbms_output.put_line(v_partition_name_7_ago);
        --             exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
        --     end if;
        --     fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        -- end loop;
        -- close cur_partition;--------------

        --只作为每次数据的中转，不保留！！！
        execute immediate 'truncate table TB_EXPORT_TMP_W';-- using V_TBNAME;

        OPEN CUR_SQL_1; --DATA_CUR
        LOOP
            FETCH CUR_SQL_1 BULK COLLECT INTO EXPORT_TAB LIMIT V_LIMIT;

            EXIT WHEN EXPORT_TAB.count = 0;
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条


            FOR I IN EXPORT_TAB.FIRST .. EXPORT_TAB.LAST
            LOOP
                --测试语句
                j := j + 1;

                --执行插入
                execute immediate 'insert /*+append*/ into TB_EXPORT_TMP_W
                values(:S_DATE, :S_HOUR, :LAC_ID, :CELL_ID, :LAC_CI,
                :W_JTL, :W_JTL_BEF,
                :W_DHL, :W_DHL_BEF,
                :W_RTWP, :W_RTWP_BEF, :W_GCJ, :W_GCJ_BEF,
                :W_RQH_CGL, :W_RQH_CGL_BEF,
                :W_HWL, :W_HWL_BEF,
                :W_DHL_NUM, :W_DHL_NUM_BEF, :W_CJ_NUM, :W_CJ_NUM_BEF
                )'
                using
                EXPORT_TAB(I).S_DATE,
                EXPORT_TAB(I).S_HOUR,
                EXPORT_TAB(I).LAC_ID,
                EXPORT_TAB(I).CELL_ID,
                EXPORT_TAB(I).LAC_CI,
                EXPORT_TAB(I).W_JTL,
                EXPORT_TAB(I).W_JTL_BEF,
                EXPORT_TAB(I).W_DHL,
                EXPORT_TAB(I).W_DHL_BEF,
                EXPORT_TAB(I).W_RTWP,
                EXPORT_TAB(I).W_RTWP_BEF,
                EXPORT_TAB(I).W_GCJ,
                EXPORT_TAB(I).W_GCJ_BEF,
                EXPORT_TAB(I).W_RQH_CGL,
                EXPORT_TAB(I).W_RQH_CGL_BEF,
                EXPORT_TAB(I).W_HWL,
                EXPORT_TAB(I).W_HWL_BEF,
                EXPORT_TAB(I).W_DHL_NUM,
                EXPORT_TAB(I).W_DHL_NUM_BEF,
                EXPORT_TAB(I).W_CJ_NUM,
                EXPORT_TAB(I).W_CJ_NUM_BEF
                ;

                if mod(j, 1000)=0 --commit every 500 times
                    then commit;
                end if;
            END LOOP;

        END LOOP;
        COMMIT;

        CLOSE CUR_SQL_1;
        dbms_output.put_line('j: '||j);


        --入库数量判断
        execute immediate 'select count(1) from '||v_tbname into v_insert_cnt;
        -- --重复率判断
        -- execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        -- group by s_date, ecgi, style having count(1)>1)'into v_insert_repeat;
        dbms_output.put_line('临时表(导出汇聚用) '||v_tbname||' 小时级数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||' 行.
        ');
    END PROC_TB_LIST_CELL_HOUR_FORMAT_W;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --in：TB_EXPORT_TMP_W
    --out：TB_EXPORT_W
    PROCEDURE PROC_TB_LIST_CELL_HOUR_FORMAT2_W(V_DATE_THRESHOLD_START VARCHAR2, V_DATE_HOUR VARCHAR2 := 23) AS --小区小时指标
    --v_high_value_vc                 varchar2(20);
    v_partition_name                varchar2(50);
    --v_partition_name_7_ago          varchar2(50);
    /*v_mr_partition_name                varchar2(50);
    v_mr_partition_name_7_ago          varchar2(50);*/
    V_TBNAME                        varchar2(100);
    V_DATE_THRESHOLD_END            varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd'); --20191004

    --V_DATE_THRESHOLD_START_7_AGO    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 7, 'yyyymmdd'); --2019/10/03 - 7 = 2019/09/26 → 20190926
    --V_DATE_THRESHOLD_END_7_AGO      varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 6, 'yyyymmdd'); --20190927
    --sql_2                           varchar2(4000);

    v_insert_cnt   number;
    --v_insert_repeat   number;
    v_ssql varchar2(500);
    v_clean_flag number;

    j number := 0;
    v_timeout   date;

    /*V_DATE_HOUR_START varchar2(5);
    V_DATE_HOUR_END varchar2(5);*/
    V_DATE_HOUR_START_1723 varchar2(5);
    V_DATE_HOUR_END_1723 varchar2(5);
    /*V_DATE_HOUR_START_1923 varchar2(5);
    V_DATE_HOUR_END_1923 varchar2(5);*/



    type FORMAT_TYPE is record
    (
        s_date          date,
        --s_hour          integer,
        lac_id          integer,
        cell_id         integer,
        lac_ci          varchar2(50),
        style           varchar2(50),
        --data_ago_cur  varchar2(4000),
        kpi_9           VARCHAR2(100),
        --kpi_9_cur       VARCHAR2(50),
        kpi_10          VARCHAR2(100),
        --kpi_10_cur      VARCHAR2(50),
        kpi_11          VARCHAR2(100),
        --kpi_11_cur      VARCHAR2(50),
        kpi_12          VARCHAR2(100),
        --kpi_12_cur      VARCHAR2(50),
        kpi_13          VARCHAR2(100),
        --kpi_13_cur      VARCHAR2(50),
        kpi_14          VARCHAR2(100),
        --kpi_14_cur      VARCHAR2(50),
        kpi_15          VARCHAR2(100),
        --kpi_15_cur      VARCHAR2(50),
        kpi_16          VARCHAR2(100),
        --kpi_16_cur      VARCHAR2(50),
        kpi_17          VARCHAR2(100),
        --kpi_17_cur      VARCHAR2(50),
        kpi_18          VARCHAR2(100),
        --kpi_18_cur      VARCHAR2(50),
        kpi_19          VARCHAR2(100),
        --kpi_19_cur      VARCHAR2(50),
        kpi_20          VARCHAR2(100),
        --kpi_20_cur      VARCHAR2(50),
        kpi_21          VARCHAR2(100),
        --kpi_21_cur      VARCHAR2(50),
        kpi_22          VARCHAR2(100),
        --kpi_22_cur      VARCHAR2(50),
        kpi_23          VARCHAR2(100),
        --kpi_23_cur      VARCHAR2(50),
        province        VARCHAR2(32),
        city            VARCHAR2(32),
        country         VARCHAR2(64),
        factory         VARCHAR2(64),
        grid            VARCHAR2(64),
        company         VARCHAR2(64),
        loop_line       VARCHAR2(64),
        rnc             VARCHAR2(64),
        pre_field1      VARCHAR2(64),
        pre_field2      VARCHAR2(64),
        pre_field3      VARCHAR2(64),
        pre_field4      VARCHAR2(256),
        pre_field5      VARCHAR2(64),
        life            VARCHAR2(64)
    );
    -- 定义基于记录的嵌套表
    TYPE NESTED_EXPORT_TYPE IS TABLE OF FORMAT_TYPE;
    -- 声明集合变量
    EXPORT_TAB          NESTED_EXPORT_TYPE;


    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 1000;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;

    /*type partition_type is record(
        table_name      varchar2(200),
        partition_name  varchar2(50),
        high_value      varchar2(100)
    );
    partition_tmp partition_type;

    cursor cur_partition is --字典表内获取非标准分区的分区名
    select table_name,t.partition_name,t.high_value
    from USER_TAB_PARTITIONS t
    where table_name = V_TBNAME and partition_name not like 'P2%'--SYS_21313
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历*/

    /*type curtyp is ref cursor;
    cur_sql_2 curtyp;*/

    --多行转多列
    cursor cur_sql_2 is
    select ex1.s_date, ex1.lac_id, ex1.cell_id, ex1.lac_ci, ex1.style,
    /*regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,1,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "9_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,1,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "9_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,2,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "10_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,2,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "10_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,3,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "11_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,3,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "11_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,4,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "12_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,4,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "12_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,5,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "13_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,5,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "13_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,5,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "14_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,5,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "14_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,6,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "15_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,6,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "15_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,7,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "16_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,7,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "16_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,8,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "17_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,8,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "17_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,9,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "18_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,9,'i') , '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "18_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,10,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "19_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,10,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "19_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,11,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "20_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,11,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "20_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,12,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "21_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,12,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "21_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,13,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "22_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,13,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "22_cur",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,14,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,1,'i') as "23_wk",
    regexp_substr(regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,14,'i'), '[^:]+',1,2,'i' ), '[^/]+',1,2,'i') as "23_cur",*/
    regexp_substr(regexp_substr(ex1.data_ago_cur, '[^,]+',1,1,'i') , '[^:]+',1,2,'i' ) as KPI_9,
    regexp_substr(regexp_substr(ex1.data_ago_cur, '[^,]+',1,2,'i') , '[^:]+',1,2,'i' ) as KPI_10,
    regexp_substr(regexp_substr(ex1.data_ago_cur, '[^,]+',1,3,'i') , '[^:]+',1,2,'i' ) as KPI_11,
    regexp_substr(regexp_substr(ex1.data_ago_cur, '[^,]+',1,4,'i') , '[^:]+',1,2,'i' ) as KPI_12,
    regexp_substr(regexp_substr(ex1.data_ago_cur, '[^,]+',1,5,'i') , '[^:]+',1,2,'i' ) as KPI_13,
    regexp_substr(regexp_substr(ex1.data_ago_cur, '[^,]+',1,6,'i') , '[^:]+',1,2,'i' ) as KPI_14,
    regexp_substr(regexp_substr(ex1.data_ago_cur, '[^,]+',1,7,'i') , '[^:]+',1,2,'i' ) as KPI_15,
    regexp_substr(regexp_substr(ex1.data_ago_cur, '[^,]+',1,8,'i') , '[^:]+',1,2,'i' ) as KPI_16,
    regexp_substr(regexp_substr(ex2.data_ago_cur, '[^,]+',1,1,'i') , '[^:]+',1,2,'i' ) as KPI_17,
    regexp_substr(regexp_substr(ex2.data_ago_cur, '[^,]+',1,2,'i') , '[^:]+',1,2,'i' ) as KPI_18,
    regexp_substr(regexp_substr(ex2.data_ago_cur, '[^,]+',1,3,'i'), '[^:]+',1,2,'i' ) as KPI_19,
    regexp_substr(regexp_substr(ex2.data_ago_cur, '[^,]+',1,4,'i'), '[^:]+',1,2,'i' ) as KPI_20,
    regexp_substr(regexp_substr(ex2.data_ago_cur, '[^,]+',1,5,'i'), '[^:]+',1,2,'i' ) as KPI_21,
    regexp_substr(regexp_substr(ex2.data_ago_cur, '[^,]+',1,6,'i'), '[^:]+',1,2,'i' ) as KPI_22,
    regexp_substr(regexp_substr(ex2.data_ago_cur, '[^,]+',1,7,'i'), '[^:]+',1,2,'i' ) as KPI_23,
    PROVINCE, CITY, COUNTRY, FACTORY, GRID, COMPANY, LOOP_LINE, RNC,
    PRE_FIELD1, PRE_FIELD2, PRE_FIELD3, PRE_FIELD4, PRE_FIELD5, LIFE
    from
    (
        select s_date, /*s_hour, */lac_id, cell_id, lac_ci, style, listagg(data_ago_cur,',') within group (order by s_hour) as data_ago_cur
        from
        (
            select s_date, s_hour, lac_id, cell_id, lac_ci, style, /*zcflag||'/'||tbflag flag_zc_tb,*/ s_hour||':'||data_wk_ago||'\'||data_cur as data_ago_cur from
            (
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G接通率' as style, /* jtl_badflag as zcflag, jtl_tbflag as tbflag, */ w_jtl as data_cur, w_jtl_bef as data_wk_ago from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G掉话率', decode(w_dhl, 0, w_dhl, to_char(w_dhl, 'fm990.00'))||'~'||w_dhl_num w_dhl, decode(w_dhl_bef, 0, w_dhl_bef, to_char(w_dhl_bef, 'fm990.00'))||'~'||w_dhl_num_bef w_dhl_bef from TB_EXPORT_TMP_W x where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G高干扰', /* avg_rtwp_badflag, avg_rtwp_tbflag, */ w_rtwp, w_rtwp_bef from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G高重建', decode(w_gcj, 0, w_gcj, to_char(w_gcj, 'fm990.00'))||'~'||w_cj_num w_gcj, decode(w_gcj_bef, 0, w_gcj_bef, to_char(w_gcj_bef, 'fm990.00'))||'~'||w_cj_num_bef w_gcj_bef from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G软切换', /* tpqh_cgl_badflag, tpqh_cgl_tbflag, */ w_rqh_cgl, w_rqh_cgl_bef from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G话务量', /* mr_badflag, mr_tbflag, */ to_char(w_hwl, 'fm990.00'), to_char(w_hwl_bef, 'fm990.00') from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
            )-- order by lac_ci, s_hour
            where s_hour <= (case when (V_DATE_HOUR is null or V_DATE_HOUR >=16) then 16 else cast(V_DATE_HOUR as int) end) --加限制，每小时出数据
            --and lac_ci = 8396812
        )  group by s_date, lac_id, cell_id, lac_ci, style--, data_ago_cur
    )ex1
    left join
    (
        select s_date, lac_ci, style, listagg(data_ago_cur,',') within group (order by s_hour) as data_ago_cur
        from
        (
            select s_date, s_hour, lac_id, cell_id, lac_ci, style, /*zcflag||'/'||tbflag flag_zc_tb,*/ s_hour||':'||data_wk_ago||'\'||data_cur as data_ago_cur from
            (
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G接通率' as style, /* jtl_badflag as zcflag, jtl_tbflag as tbflag, */ w_jtl as data_cur, w_jtl_bef as data_wk_ago from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G掉话率', decode(w_dhl, 0, w_dhl, to_char(w_dhl, 'fm990.00'))||'~'||w_dhl_num w_dhl, decode(w_dhl_bef, 0, w_dhl_bef, to_char(w_dhl_bef, 'fm990.00'))||'~'||w_dhl_num_bef w_dhl_bef from TB_EXPORT_TMP_W x where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G高干扰', /* avg_rtwp_badflag, avg_rtwp_tbflag, */ w_rtwp, w_rtwp_bef from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G高重建', decode(w_gcj, 0, w_gcj, to_char(w_gcj, 'fm990.00'))||'~'||w_cj_num w_gcj, decode(w_gcj_bef, 0, w_gcj_bef, to_char(w_gcj_bef, 'fm990.00'))||'~'||w_cj_num_bef w_gcj_bef from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G软切换', /* tpqh_cgl_badflag, tpqh_cgl_tbflag, */ w_rqh_cgl, w_rqh_cgl_bef from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, lac_id, cell_id, lac_ci, '3G话务量', /* mr_badflag, mr_tbflag, */ to_char(w_hwl, 'fm990.00'), to_char(w_hwl_bef, 'fm990.00') from TB_EXPORT_TMP_W where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
            )-- order by lac_ci, s_hour
            -- where s_hour <= (case when (V_DATE_HOUR is null or V_DATE_HOUR >=16) then 16 else cast(V_DATE_HOUR as int) end) --加限制，每小时出数据
            --and lac_ci = 8396812
        )  group by s_date, lac_id, cell_id, lac_ci, style--, data_ago_cur
    )ex2
    on ex1.lac_ci = ex2.lac_ci and ex1.s_date = ex2.s_date and ex1.style =ex2.style
    left join
    (
        select lac||'-'||ci as lac_ci,
        t.* from DT_CELL_W t
    )DT
    on ex1.lac_ci = DT.lac_ci;


    BEGIN

       --程序超时判定的起始时间设置
        v_timeout := sysdate;

        /*if V_DATE_HOUR is null
            then  V_DATE_HOUR_START := 17;
                     V_DATE_HOUR_END := 23;
        elsif V_DATE_HOUR <= 16
            then  V_DATE_HOUR_START := 404;
                     V_DATE_HOUR_END := 404;
        elsif V_DATE_HOUR >= 17
            then  V_DATE_HOUR_START := 17;
                     V_DATE_HOUR_END := V_DATE_HOUR;
        end if;*/

        if V_DATE_HOUR is null --入口参数2：V_DATE_HOUR：NULL
            then    V_DATE_HOUR_START_1723 := 17;
                    V_DATE_HOUR_END_1723 := 23;
                    /*V_DATE_HOUR_START_1923 := 19;
                    V_DATE_HOUR_END_1923 := 23;*/
        elsif V_DATE_HOUR < 17 --9~13
            then    V_DATE_HOUR_START_1723 := 404;
                    V_DATE_HOUR_END_1723 := 404;
                    /*V_DATE_HOUR_START_1923 := 404;
                    V_DATE_HOUR_END_1923 := 404;*/
        elsif V_DATE_HOUR >= 17 and V_DATE_HOUR <= 23 --14~18
            then    V_DATE_HOUR_START_1723 := 17;
                    V_DATE_HOUR_END_1723 := V_DATE_HOUR;
        end if;



        V_TBNAME := 'TB_EXPORT_W';
        j := 0;
        --索引分区名
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);        
        /*open cur_partition; --开始索引字典表
        fetch cur_partition into partition_tmp;
        loop--------------
            exit when NOT (cur_partition%FOUND);
            v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
            if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    v_partition_name := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
            end if;
            fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        end loop;
        close cur_partition;--------------*/

        --清理
       if V_PARTITION_NAME <> 'NULL'
       then
            execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
            while v_clean_flag !=0 loop
                select
                'CALL PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                where s.table_name = V_TBNAME;
                execute immediate v_ssql;
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||V_PARTITION_NAME||')' into v_clean_flag;
            end loop;
        end if;
        --准备工作完成！！


        ---------
        OPEN CUR_SQL_2; --DATA_CUR
        LOOP
            FETCH CUR_SQL_2 BULK COLLECT INTO EXPORT_TAB LIMIT V_LIMIT;
            EXIT WHEN EXPORT_TAB.count = 0;
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条


            FOR I IN EXPORT_TAB.FIRST .. EXPORT_TAB.LAST
            LOOP

                j := j + 1;

                --执行插入
                execute immediate 'insert /*+append*/ into TB_EXPORT_W
                values(
                :s_date,
                :lac_id,
                :cell_id,
                :lac_ci,
                :style,
                :kpi_9,
                :kpi_10,
                :kpi_11,
                :kpi_12,
                :kpi_13,
                :kpi_14,
                :kpi_15,
                :kpi_16,
                :kpi_17,
                :kpi_18,
                :kpi_19,
                :kpi_20,
                :kpi_21,
                :kpi_22,
                :kpi_23,
                :PROVINCE, :CITY, :COUNTRY, :FACTORY, :GRID,
                :COMPANY, :LOOP_LINE, :RNC,
                :PRE_FIELD1, :PRE_FIELD2, :PRE_FIELD3, :PRE_FIELD4, :PRE_FIELD5, :LIFE)'
                using
                EXPORT_TAB(I).s_date,
                --EXPORT_TAB(I).s_hour,
                EXPORT_TAB(I).lac_id,
                EXPORT_TAB(I).cell_id,
                EXPORT_TAB(I).lac_ci,
                EXPORT_TAB(I).style,
                /*EXPORT_TAB(I).data_ago_cur,*/
                EXPORT_TAB(I).kpi_9,
                EXPORT_TAB(I).kpi_10,
                EXPORT_TAB(I).kpi_11,
                EXPORT_TAB(I).kpi_12,
                EXPORT_TAB(I).kpi_13,
                EXPORT_TAB(I).kpi_14,
                EXPORT_TAB(I).kpi_15,
                EXPORT_TAB(I).kpi_16,
                EXPORT_TAB(I).kpi_17,
                EXPORT_TAB(I).kpi_18,
                EXPORT_TAB(I).kpi_19,
                EXPORT_TAB(I).kpi_20,
                EXPORT_TAB(I).kpi_21,
                EXPORT_TAB(I).kpi_22,
                EXPORT_TAB(I).kpi_23,
                EXPORT_TAB(I).PROVINCE, EXPORT_TAB(I).CITY, EXPORT_TAB(I).COUNTRY,
                EXPORT_TAB(I).FACTORY, EXPORT_TAB(I).GRID,
                EXPORT_TAB(I).COMPANY, EXPORT_TAB(I).LOOP_LINE, EXPORT_TAB(I).RNC,
                EXPORT_TAB(I).PRE_FIELD1, EXPORT_TAB(I).PRE_FIELD2,
                EXPORT_TAB(I).PRE_FIELD3, EXPORT_TAB(I).PRE_FIELD4,
                EXPORT_TAB(I).PRE_FIELD5, EXPORT_TAB(I).LIFE;

                if mod(j, 1000)=0 --commit every 100 times
                    then commit;
                    if mod(j, 100000) = 0
                    then dbms_output.put_line('j: '||j);
                    end if;
                end if;

                --超时判定
                if round(to_number(sysdate - v_timeout) * 24 * 60) >= 13 then return;
                end if;
            END LOOP;

        END LOOP;
        commit;

        --if sysdate - v_timeout
        close cur_sql_2;
        /*--超时判定
        if round(to_number(sysdate - v_timeout) * 24 * 60) >= 13 then return;
        end if;*/

        dbms_output.put_line('j: '||j);

        --入库数量判断
        execute immediate 'select count(1) from '||v_tbname||' partition('||V_PARTITION_NAME||')' into v_insert_cnt;
        --重复率判断
        /*execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        group by s_date, lac_ci, style having count(1)>1)'into v_insert_repeat;*/
        dbms_output.put_line('表 '||v_tbname||' 天级数据迭代更新完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||' 行.
        ');
    END PROC_TB_LIST_CELL_HOUR_FORMAT2_W;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --TB指标结果表周期性汇聚任务激活
    PROCEDURE ACTIVE_LC_ZCTB_W_AUTO AS
    V_DATE_START  varchar2(15);--天表任务激活时间戳
    V_DATE_HOUR    varchar2(15);--天表小时粒度激活时间戳
    V_DATE_START_8  varchar2(15);--天表任务激活时间戳
    
    /*V_DATE_MONDAY date;
    V_DATE_MONTH   date;*/
    v_loop_log number := 0;
    /*v_inside_loop_log number := 0;
    v_tbname varchar2(50);
    v_partition_name varchar2(30);
    v_exsit_flag number := 0;
    v_pkg_name varchar2(200);*/

    BEGIN
        --起止时间戳格式化，时间自动化，读取时间：sysdate--2019/10/22
        V_DATE_START :=  to_char(sysdate - numtodsinterval(2,'hour'),'yyyymmdd');--20191022
        V_DATE_HOUR  := /*23;*/ to_number(to_char(sysdate - numtodsinterval(2,'hour'), 'hh24'));--now：10:21 --->  8

        V_DATE_START_8 :=  to_char(sysdate-1,'yyyymmdd');--20191022

        --每日执行昨日天级汇聚
        /*v_tbname := 'LC_INDEX_VOLTE_8';
        v_pkg_name := 'PKG_LC_INDEX_VOLTE_CELL.PROC_LC_INDEX_VOLTE_8';

        --从系统中获取待插入分区名
         select t.partition_name into v_partition_name from USER_TAB_PARTITIONS t
        where t.table_name = v_tbname
        --and t.partition_name like 'P\_%' escape '\' --分区名严格遵守时，可忽略此行条件
        and regexp_substr(t.partition_name,'[^_]+',1,2,'i') = V_DATE_START; */

        --这里三表的分区名均严格遵守分区格式，故只需索引一张表即可获取其余表的待处理分区名，P_20190825...
        /* LC_INDEX_VOLTE_8
        execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||')' into v_exsit_flag;
        while v_exsit_flag = 0 and v_partition_name is not null loop
          exit when v_inside_loop_log >=5;
          PKG_LC_INDEX_VOLTE_CELL.PROC_LC_INDEX_VOLTE_8(V_DATE_START);
          execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||')' into v_exsit_flag;
          v_inside_loop_log := v_inside_loop_log +1;
        end loop;
        --log
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_LOGGING(sysdate, v_pkg_name, v_inside_loop_log, v_exsit_flag); */
        if V_DATE_HOUR >= 9
            then
            PKG_LC_INDEX_TB_CELL_W.PROC_TB_LIST_CELL_HOUR_W(V_DATE_START, V_DATE_HOUR);
            v_loop_log := v_loop_log +1;

            PKG_LC_INDEX_TB_CELL_W.PROC_TB_INDEX_CELL_DAY_W(V_DATE_START);
            v_loop_log := v_loop_log +1;

            PKG_LC_INDEX_TB_CELL_W.PROC_TB_LIST_CELL_HOUR_FORMAT_W(V_DATE_START);
            v_loop_log := v_loop_log +1;

            PKG_LC_INDEX_TB_CELL_W.PROC_TB_LIST_CELL_HOUR_FORMAT2_W(V_DATE_START);
            v_loop_log := v_loop_log +1;

            dbms_output.put_line('WCDMA-TB小区小时级清单/天级标签周期性汇聚任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
            dbms_output.put_line('****------------------------------------------------------------------****
            ');
        elsif V_DATE_HOUR = 2
            then 
            PKG_LC_INDEX_TB_CELL_W.PROC_TB_OSS_DAY_W(V_DATE_START_8);
            v_loop_log := v_loop_log +1;
            dbms_output.put_line('WCDMA-TB小区天级区域数量统计任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
        else 
                dbms_output.put_line('WCDMA-TB周期性汇聚任务触发时间未至！！！：');
                return;
        end if;

    END ACTIVE_LC_ZCTB_W_AUTO;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --TB数据补偿
    PROCEDURE ACTIVE_LC_ZCTB_W_SUPPLEMENT(V_DATE_THRESHOLD_START VARCHAR2) AS
    --V_DATE_START  varchar2(15);
    --V_DATE_MONDAY date;
    --V_DATE_MONTH   date;
    v_loop_log number := 0;
    BEGIN
        --起止时间戳格式化，时间自动化，读取时间：sysdate--2019/07/01
        --V_DATE_START :=  to_char(sysdate-1,'yyyymmdd');--20190630
        --每日执行昨日天级汇聚
        PKG_LC_INDEX_TB_CELL_W.PROC_TB_LIST_CELL_HOUR_W(V_DATE_THRESHOLD_START, '');
        v_loop_log := v_loop_log +1;

        PKG_LC_INDEX_TB_CELL_W.PROC_TB_INDEX_CELL_DAY_W(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;
            
        PKG_LC_INDEX_TB_CELL_W.PROC_TB_OSS_DAY_W(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;
        
        PKG_LC_INDEX_TB_CELL_W.PROC_TB_LIST_CELL_HOUR_FORMAT_W(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;

        PKG_LC_INDEX_TB_CELL_W.PROC_TB_LIST_CELL_HOUR_FORMAT2_W(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;

        dbms_output.put_line('WCDMA-TB小区小时级清单&天级标签标补偿任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
        dbms_output.put_line('****------------------------------------------------------------------****
        ');
    END ACTIVE_LC_ZCTB_W_SUPPLEMENT;

END PKG_LC_INDEX_TB_CELL_W;

/

