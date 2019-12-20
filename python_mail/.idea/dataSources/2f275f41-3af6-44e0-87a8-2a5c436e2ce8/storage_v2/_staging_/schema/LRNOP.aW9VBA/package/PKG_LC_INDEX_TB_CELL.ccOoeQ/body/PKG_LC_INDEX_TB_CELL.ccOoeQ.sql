CREATE OR REPLACE PACKAGE BODY PKG_LC_INDEX_TB_CELL AS
    --in：OMC_LTE_3 / MR_LTE_MRS_CELL_3 / DT_CELL_L
    --out：TB_LIST_CELL_HOUR
    PROCEDURE PROC_TB_LIST_CELL_HOUR(V_DATE_THRESHOLD_START VARCHAR2, V_DATE_HOUR VARCHAR2 := NULL) IS --小区小时指标
    --v_high_value_vc                 varchar2(20);
    v_partition_name                varchar2(50);
    --v_partition_name_7_ago          varchar2(50);
    --v_mr_partition_name                varchar2(50);
    --v_mr_partition_name_7_ago          varchar2(50);
    --V_DATE_THRESHOLD_START_7_AGO    varchar2(8);
    V_TBNAME                        varchar2(100);
    V_DATE_THRESHOLD_END    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd');--20190927
    --v_s_date_7            date;
    --sql_2                   varchar2(4000);
    --V_DATE_THRESHOLD_HOUR                varchar2(20);

    v_insert_cnt   number;
    v_insert_repeat   number;
    v_ssql varchar2(500);
    v_clean_flag number;
    --i_date_hour varchar2(20);

    i number := 0;
    -- c_high_value varchar2(200);
    -- c_table_name varchar2(50);
    -- c_partition_name varchar2(80);

    type omc_3_type is record(
        s_date          date,
        s_hour          integer,
        enb_id          integer,
        cell_id         integer,
        ecgi            integer,
        LTE_JTL         number,
        JTL_QUEST_NUM   number,
        LTE_DXL         number,
        DXL_DX_NUM      number,
        AVG_RTWP        number,
        LTE_LL          number,
        LL_QAM_ZB       number,
        LL_GZSL         number,
        CSFB_CGL        number,
        CSFB_QUEST_NUM  number,
        TPQH_CGL        number,
        TPQH_QUEST_NUM  number,
        TA              number,

        n4_0001         number,
        n4_0002         number,
        n4_0003         number,
        n4_0004         number,
        n4_0005         number,
        n4_0006         number,
        n4_0033         number,
        n4_0050         number,
        n4_0027         number,
        n4_0037         number,
        n4_0036         number,
        n4_0032         number,
        n4_0023         number,
        n4_0024         number,
        n4_0041         number,
        n4_0042         number,
        n4_0045         number,

        RSRP_110_RATE   number,
        RSRP_ALL_SIMPLES      number,
        PROVINCE        varchar2(32),
        VENDOR_CELL_ID  varchar2(32),
        COUNTY          varchar2(32),
        VENDOR_ID       varchar2(32),
        RESERVED3       varchar2(32),
        RESERVED8       varchar2(32),
        TOWN_ID         varchar2(32),
        RNC             varchar2(32),
        RESERVED4       varchar2(32),
        RESERVED5       varchar2(32),
        COVER_TYPE      varchar2(32),
        LIFE            varchar2(32),
        LON             number,
        LAT             number
    );
    omc3_tmp_1 omc_3_type;

    JTL_BADFLAG         varchar2(10);
    DXL_BADFLAG         varchar2(10);
    AVG_RTWP_BADFLAG    varchar2(10);
    CSFB_CGL_BADFLAG    varchar2(10);
    TPQH_CGL_BADFLAG    varchar2(10);
    MR_BADFLAG          varchar2(10);

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

    cursor cur_sql_1 is
    select omc.*, mr.RSRP_110_RATE, mr.RSRP_ALL_SIMPLES,
/*    case when omc.LTE_JTL < 90 and omc.JTL_QUEST_NUM > 1000 then 1 else 0 end JTL_BADFLAG,
    case when omc.LTE_DXL > 5  and omc.DXL_DX_NUM > 200 then 1 else 0 end DXL_BADFLAG,
    case when omc.AVG_RTWP > -95 then 1 else 0 end AVG_RTWP_BADFLAG,
    case when omc.CSFB_CGL < 90 and omc.CSFB_QUEST_NUM > 20 then 1 else 0 end CSFB_CGL_BADFLAG,
    case when omc.TPQH_CGL < 80 and omc.TPQH_QUEST_NUM > 100 then 1 else 0 end TPQH_CGL_BADFLAG,
    case when mr.RSRP_110_RATE < 80 and mr.RSRP_ALL_SIMPLES > 5000 then 1 else 0 end MR_BADFLAG,*/
    PROVINCE, VENDOR_CELL_ID, COUNTY, VENDOR_ID, RESERVED3, RESERVED8,
    TOWN_ID, RNC, RESERVED4, RESERVED5, COVER_TYPE, /*PRE_FIELD4, PRE_FIELD5,*/ LIFE, LON, LAT from
    (
      select
      s_date, s_hour, omc_enb_id as enb_id, omc_cell_id as cell_id, omc_ecgi as ecgi,
      100*decode((n4_0002*n4_0004),0,null,null,null, round(n4_0001*n4_0003/(n4_0002*n4_0004), 4)) as LTE_JTL,
      decode(n4_0002,null,null, n4_0002) as JTL_QUEST_NUM,
      100*decode(n4_0006,0,null,null,null, round(n4_0005/n4_0006, 4)) as LTE_DXL,
      decode(n4_0005,null,null, n4_0005) as DXL_DX_NUM,
      decode(n4_0033,0,null,null,null, round(n4_0033, 2)) as AVG_RTWP,  --0的话为错误数据
      round((decode(n4_0050,null,0, n4_0050) + decode(n4_0027,null,0, n4_0027))*1024*1024/1000/1000/1000, 2) as LTE_LL,
      100*decode(n4_0037,0,null,null,null, round(n4_0036/n4_0037, 4)) as LL_QAM_ZB,
      round(n4_0032*1024*1024/1000/1000, 2) as LL_GZSL,
      100*decode(n4_0024,0,null,null,null, round(n4_0023/n4_0024, 4)) as CSFB_CGL,
      decode(n4_0024,null,0, n4_0024) as CSFB_QUEST_NUM,
      100*decode(n4_0042,0,null,null,null, round(n4_0041/n4_0042, 4)) as TPQH_CGL,
      decode(n4_0042,null,0, n4_0042) as TPQH_QUEST_NUM,
      round(n4_0045, 2) as TA,
      n4_0001, n4_0002, n4_0003, n4_0004,
      n4_0005, n4_0006,
      n4_0033,
      n4_0050, n4_0027,
      n4_0037, n4_0036, n4_0032,
      n4_0023, n4_0024,
      n4_0041, n4_0042,
      n4_0045
      from
      (
        select s_date, s_hour,
        enb_id as omc_enb_id, cell_id as omc_cell_id, ecgi as omc_ecgi, vendor,
        n4_0001, n4_0002, n4_0003, n4_0004,
        n4_0005, n4_0006,
        n4_0033,
        n4_0050, n4_0027,
        n4_0036, n4_0037,
        n4_0032,
        n4_0023, n4_0024,
        n4_0041, n4_0042,
        n4_0045
        from omc_lte_3 where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
        and s_hour between 9 and 23 
        and s_hour = (case when V_DATE_HOUR is null then s_hour else cast(V_DATE_HOUR as int) end)
      )
    )omc
    left join
    (
        select t.enb_id*256 + t.ci as ecgi, t.* from dt_cell_l t
    ) dt
    on omc.ecgi = dt.ecgi
    left join
    (
      select
      s_date,
      s_hour,
      mrs_ecgi,
      100*decode((RSRP00+RSRP01+RSRP02_04+RSRP05_06+RSRP07_11+RSRP12_16+RSRP17_21+RSRP22_26+RSRP27_31+RSRP32_36+RSRP37_41+RSRP42_46+RSRP47),0,null,null,null,
      round((RSRP07_11+RSRP12_16+RSRP17_21+RSRP22_26+RSRP27_31+RSRP32_36+RSRP37_41+RSRP42_46+RSRP47)/
      (RSRP00+RSRP01+RSRP02_04+RSRP05_06+RSRP07_11+RSRP12_16+RSRP17_21+RSRP22_26+RSRP27_31+RSRP32_36+RSRP37_41+RSRP42_46+RSRP47), 4) ) as RSRP_110_RATE,
      (RSRP00+RSRP01+RSRP02_04+RSRP05_06+RSRP07_11+RSRP12_16+RSRP17_21+RSRP22_26+RSRP27_31+RSRP32_36+RSRP37_41+RSRP42_46+RSRP47) as RSRP_ALL_SIMPLES
      from
      (
        select trunc(start_time) s_date,
        cast(s_hour as int) s_hour,
        regexp_substr(t.cell_uk, '[^-]+',1,1,'i') as enb_id, regexp_substr(t.cell_uk, '[^-]+',1,2,'i') as cell_id,
        --regexp_substr(t.cell_uk, '[^-]+',1,1,'i')*256 + regexp_substr(t.cell_uk, '[^-]+',1,2,'i') mrs_ecgi,
        ecgi mrs_ecgi,
        rsrp00, rsrp01, rsrp02_04, rsrp05_06, 
        rsrp07_11, rsrp12_16, rsrp17_21, rsrp22_26, 
        rsrp27_31, rsrp32_36, rsrp37_41, rsrp42_46, 
        rsrp47
        from mr_lte_mrs_cell_3 t 
        where start_time >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and start_time < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
      )where s_hour = (case when V_DATE_HOUR is null then s_hour else cast(V_DATE_HOUR as int) end) 
      and s_hour between 9 and 23 
    )mr
    on omc.ecgi = mr.mrs_ecgi and omc.s_date = mr.s_date and omc.s_hour = mr.s_hour;

    BEGIN
        --起止时间戳格式化
        --v_date_start := to_date(V_DATE_THRESHOLD_START,'yyyymmdd');--起始时间戳'yyyymmdd'格式化
        --v_date_end := v_date_start + 1;--起始时间戳'yyyymmdd'格式化
        V_TBNAME := 'TB_LIST_CELL_HOUR';

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
              --清理
              execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||') where s_hour='||V_DATE_HOUR into v_clean_flag;
              while v_clean_flag !=0 loop
                  --若入口参数指定了小时，则仅重跑指定小时对应的数据，此处索引至天级分区，并按小时清除
                  execute immediate 'delete from '||v_tbname||' partition('||v_partition_name||') where s_hour='||V_DATE_HOUR;
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
              --清理
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


        /*V_TBNAME := 'OMC_LTE_3';
        V_DATE_THRESHOLD_START_7_AGO := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd')-7, 'yyyymmdd'); --2019/10/03 - 7 = 2019/09/26 → 2019-09-26

        --索引分区名
        open cur_partition; --开始索引字典表
        fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        loop--------------
            exit when NOT (cur_partition%FOUND);
            v_high_value_vc := substr(partition_tmp.high_value, 12, 10); --less than 2019-07-14 ...
            if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    v_partition_name := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name);
            elsif (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START_7_AGO)
                then
                    v_partition_name_7_ago := partition_tmp.partition_name;
                    dbms_output.put_line(v_partition_name_7_ago);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
            end if;
            fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        end loop;
        close cur_partition;--------------

        V_TBNAME := 'MR_LTE_MRS_CELL_3';
        open cur_partition; --开始索引字典表
        fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        loop--------------
            exit when NOT (cur_partition%FOUND);
            v_high_value_vc := substr(partition_tmp.high_value, 11, 10); --less than 2019-07-14 ...
            if (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START)
                then
                    v_mr_partition_name := partition_tmp.partition_name;
                    dbms_output.put_line(v_mr_partition_name);
            elsif (to_char(to_date(v_high_value_vc,'yyyy-mm-dd')-1, 'yyyymmdd') = V_DATE_THRESHOLD_START_7_AGO)
                then
                    v_mr_partition_name_7_ago := partition_tmp.partition_name;
                    dbms_output.put_line(v_mr_partition_name_7_ago);
                    exit;--降序遍历，达到目标分区值时(指定时间&指定时间一周前)，游标结束，退出！！！抛弃多余遍历
            end if;
            fetch cur_partition into partition_tmp.table_name, partition_tmp.partition_name, partition_tmp.high_value;
        end loop;
        close cur_partition;--------------*/


        /*begin
            if V_DATE_HOUR is null then V_DATE_THRESHOLD_HOUR := 'S_HOUR';
            else V_DATE_THRESHOLD_HOUR := V_DATE_HOUR;
            end if;
        end;*/

        --开始
        open cur_sql_1; --data_cur
        fetch cur_sql_1 into omc3_tmp_1;
        --v_s_date_7 := omc3_tmp_1.s_date - 7;
        loop
            exit when NOT (cur_sql_1%FOUND);
            --标签初始化
            JTL_BADFLAG := 0;
            DXL_BADFLAG := 0;
            AVG_RTWP_BADFLAG := 0;
            --LL_BADFLAG  := 0;
            CSFB_CGL_BADFLAG := 0;
            TPQH_CGL_BADFLAG := 0;
            MR_BADFLAG := 0;

            --测试语句
            i := i + 1;
            --dbms_output.put_line('i: '||i);
            --dbms_output.put_line(kpi_tmp_2.ecgi||'-'||to_char(kpi_tmp_2.s_date, 'yyyymmdd')||'-'||kpi_tmp_2.s_hour);--当前时间戳对应的小区小时数据
            --dbms_output.put_line(omc3_tmp_1.ecgi||'-'||to_char(omc3_tmp_1.s_date, 'yyyymmdd')||'-'||omc3_tmp_1.s_hour);--7日前对应小区小时数据

            --开始进行小区小时级标签判断
            --某小区某小时
            if omc3_tmp_1.LTE_JTL < 90 and omc3_tmp_1.JTL_QUEST_NUM > 1000
                then JTL_BADFLAG := 1;
            end if;
            if omc3_tmp_1.LTE_DXL > 5  and omc3_tmp_1.DXL_DX_NUM > 200
                then DXL_BADFLAG := 1;
            end if;
            if omc3_tmp_1.AVG_RTWP > -95
                then AVG_RTWP_BADFLAG := 1;
            end if;
            if omc3_tmp_1.CSFB_CGL < 90 and omc3_tmp_1.CSFB_QUEST_NUM > 10
                then CSFB_CGL_BADFLAG := 1;
            end if;
            if omc3_tmp_1.TPQH_CGL < 80 and omc3_tmp_1.TPQH_QUEST_NUM > 100
                then TPQH_CGL_BADFLAG := 1;
            end if;
            if omc3_tmp_1.RSRP_110_RATE < 80 and omc3_tmp_1.RSRP_ALL_SIMPLES > 5000
                then MR_BADFLAG := 1;
            end if;

            --执行插入
            execute immediate 'insert /*+append*/ into TB_LIST_CELL_HOUR
            values(:s_date, :s_hour, :enb_id, :cell_id, :ecgi,
            :LTE_JTL, :JTL_QUEST_NUM,
            :LTE_DXL, :DXL_DX_NUM,
            :AVG_RTWP,
            :LTE_LL, :LL_QAM_ZB, :LL_GZSL,
            :CSFB_CGL, :CSFB_QUEST_NUM,
            :TPQH_CGL, :TPQH_QUEST_NUM,
            :TA,
            :n4_0001, :n4_0002, :n4_0003, :n4_0004,
            :n4_0005, :n4_0006,
            :n4_0033,
            :n4_0050, :n4_0027,
            :n4_0037, :n4_0036, :n4_0032,
            :n4_0023, :n4_0024,
            :n4_0041, :n4_0042,
            :n4_0045,
            :RSRP_110_RATE,
            :RSRP_ALL_SIMPLES,
            :JTL_BADFLAG,
            :DXL_BADFLAG,
            :AVG_RTWP_BADFLAG,
            :CSFB_CGL_BADFLAG,
            :TPQH_CGL_BADFLAG,
            :MR_BADFLAG,
            :PROVINCE, :VENDOR_CELL_ID, :COUNTY, :VENDOR_ID, :RESERVED3, :RESERVED8,
            :TOWN_ID, :RNC, :RESERVED4, :RESERVED5, :COVER_TYPE, :LIFE, :LON, :LAT)'
            using
            omc3_tmp_1.s_date, omc3_tmp_1.s_hour, omc3_tmp_1.enb_id, omc3_tmp_1.cell_id, omc3_tmp_1.ecgi,
            omc3_tmp_1.LTE_JTL, omc3_tmp_1.JTL_QUEST_NUM,
            omc3_tmp_1.LTE_DXL, omc3_tmp_1.DXL_DX_NUM,
            omc3_tmp_1.AVG_RTWP,
            omc3_tmp_1.LTE_LL, omc3_tmp_1.LL_QAM_ZB, omc3_tmp_1.LL_GZSL,
            omc3_tmp_1.CSFB_CGL, omc3_tmp_1.CSFB_QUEST_NUM,
            omc3_tmp_1.TPQH_CGL, omc3_tmp_1.TPQH_QUEST_NUM,
            omc3_tmp_1.TA,
            omc3_tmp_1.n4_0001, omc3_tmp_1.n4_0002, omc3_tmp_1.n4_0003, omc3_tmp_1.n4_0004,
            omc3_tmp_1.n4_0005, omc3_tmp_1.n4_0006,
            omc3_tmp_1.n4_0033,
            omc3_tmp_1.n4_0050, omc3_tmp_1.n4_0027,
            omc3_tmp_1.n4_0037, omc3_tmp_1.n4_0036, omc3_tmp_1.n4_0032,
            omc3_tmp_1.n4_0023, omc3_tmp_1.n4_0024,
            omc3_tmp_1.n4_0041, omc3_tmp_1.n4_0042,
            omc3_tmp_1.n4_0045,
            omc3_tmp_1.RSRP_110_RATE, omc3_tmp_1.RSRP_ALL_SIMPLES,
            JTL_BADFLAG, DXL_BADFLAG,
            AVG_RTWP_BADFLAG, CSFB_CGL_BADFLAG,
            TPQH_CGL_BADFLAG,
            MR_BADFLAG,
            omc3_tmp_1.PROVINCE, omc3_tmp_1.VENDOR_CELL_ID, omc3_tmp_1.COUNTY,
            omc3_tmp_1.VENDOR_ID, omc3_tmp_1.RESERVED3, omc3_tmp_1.RESERVED8,
            omc3_tmp_1.TOWN_ID, omc3_tmp_1.RNC, omc3_tmp_1.RESERVED4, omc3_tmp_1.RESERVED5, omc3_tmp_1.COVER_TYPE,
            omc3_tmp_1.LIFE, omc3_tmp_1.LON, omc3_tmp_1.LAT;

            if mod(i, 100)=0 --commit every 100 times
                then commit;
            end if;

            --该小区下一小时
            fetch cur_sql_1 into omc3_tmp_1;


        end loop;
        commit;

        close cur_sql_1;
        dbms_output.put_line('i: '||i);

        --入库数量判断
        execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||')' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        group by s_date, s_hour, ecgi having count(1)>1)'into v_insert_repeat;
        dbms_output.put_line('表 '||v_tbname||' 小时数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');

    END PROC_TB_LIST_CELL_HOUR;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --in：TB_LIST_CELL_HOUR
    --out：TB_LIST_CELL_HOUR_TBZC
    PROCEDURE PROC_TB_LIST_CELL_HOUR_TBZC(V_DATE_THRESHOLD_START VARCHAR2, V_DATE_HOUR VARCHAR2 := NULL) IS --小区小时指标
    --v_high_value_vc                 varchar2(20);
    v_partition_name                varchar2(50);
    --v_partition_name_7_ago          varchar2(50);
    /*v_mr_partition_name                varchar2(50);
    v_mr_partition_name_7_ago          varchar2(50);*/
    V_TBNAME                        varchar2(100);
    V_DATE_THRESHOLD_END            varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd'); --20191004

    V_DATE_THRESHOLD_START_7_AGO    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 7, 'yyyymmdd'); --2019/10/03 - 7 = 2019/09/26 → 20190926
    V_DATE_THRESHOLD_END_7_AGO      varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 6, 'yyyymmdd'); --20190927
    --sql_2                           varchar2(4000);


    v_insert_cnt   number;
    v_insert_repeat   number;
    v_ssql varchar2(500);
    v_clean_flag number;
    --i_date_hour varchar2(20);


    i number := 0;

    type tbzc_list_3_type is record(
        s_date          date,
        s_hour          integer,
        enb_id          integer,
        cell_id         integer,
        ecgi            integer,
        JTL_BADFLAG         varchar2(5),
        DXL_BADFLAG         varchar2(5),
        AVG_RTWP_BADFLAG    varchar2(5),
        CSFB_CGL_BADFLAG    varchar2(5),
        TPQH_CGL_BADFLAG    varchar2(5),
        MR_BADFLAG          varchar2(5),

        JTL_TBFLAG          varchar2(5),
        DXL_TBFLAG          varchar2(5),
        AVG_RTWP_TBFLAG     varchar2(5),
        LL_TBFLAG           varchar2(5),
        CSFB_CGL_TBFLAG     varchar2(5),
        TPQH_CGL_TBFLAG     varchar2(5),
        TA_TBFLAG           varchar2(5),
        MR_TBFLAG           varchar2(5),

        LTE_JTL         number,
        LTE_JTL_N       number,
        LTE_JTL_D       number,

        LTE_DXL         number,
        LTE_DXL_N       number,
        LTE_DXL_D       number,

        AVG_RTWP        number,

        LTE_LL          number,
        LL_QAM_ZB       number,
        LL_GZSL         number,

        CSFB_CGL        number,
        CSFB_CGL_N      number,
        CSFB_CGL_D      number,


        TPQH_CGL        number,
        TPQH_CGL_N      number,
        TPQH_CGL_D      number,
        TA              number,

        RSRP_110_RATE   number,
        RSRP_ALL_SIMPLES    number,

        PROVINCE        varchar2(32),
        VENDOR_CELL_ID  varchar2(32),
        COUNTY          varchar2(32),
        VENDOR_ID       varchar2(32),
        RESERVED3       varchar2(32),
        RESERVED8       varchar2(32),
        TOWN_ID         varchar2(32),
        RNC             varchar2(32),
        RESERVED4       varchar2(32),
        RESERVED5       varchar2(32),
        COVER_TYPE      varchar2(32),
        LIFE            varchar2(32),
        LON             number,
        LAT             number
    );
    list_tmp_1 tbzc_list_3_type;
    /*list_new tbzc_list_3_type;
    list_old tbzc_list_3_type;*/


    JTL_TBFLAG         varchar2(5);
    DXL_TBFLAG         varchar2(5);
    AVG_RTWP_TBFLAG    varchar2(5);
    LL_TBFLAG          varchar2(5);
    CSFB_CGL_TBFLAG    varchar2(5);
    TPQH_CGL_TBFLAG    varchar2(5);
    TA_TBFLAG          varchar2(5);
    MR_TBFLAG          varchar2(5);

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

    -- type curtyp is ref cursor;
    -- cur_sql_2 curtyp;

    -- type end_date_type is record(
    --     cur_sql_1_end_flag        INTEGER,
    --     cur_sql_1_hour_end_flag        INTEGER
    -- );
    -- end_flag end_date_type;


    --获取单日不同小区号及其附属信息
    cursor cur_sql_1 is
    select tnow.s_date, tnow.s_hour, tnow.enb_id, tnow.cell_id, tnow.ecgi,
    tnow.JTL_BADFLAG,
    tnow.DXL_BADFLAG,
    tnow.AVG_RTWP_BADFLAG,
    tnow.CSFB_CGL_BADFLAG,
    tnow.TPQH_CGL_BADFLAG,
    tnow.MR_BADFLAG,
    case when tbef.LTE_JTL > 1000 and tbef.LTE_JTL - tnow.LTE_JTL > 5 then 1 else 0 end as JTL_TBFLAG,
    case when tbef.DXL_DX_NUM > 100 and decode(tbef.DXL_DX_NUM, 0,null,null,null, tnow.DXL_DX_NUM/ tbef.DXL_DX_NUM) >= 2 then 1 else 0 end as DXL_TBFLAG,
    case when tnow.AVG_RTWP - tbef.AVG_RTWP >15 then 1 else 0 end as AVG_RTWP_TBFLAG,
    case when tbef.LTE_LL > 1
        and decode(tnow.LTE_LL,0,null,null,null, (tbef.LTE_LL - tnow.LTE_LL)/ tnow.LTE_LL) > 0.7
        and decode(tbef.LL_QAM_ZB,0,null,null,null, (tbef.LL_QAM_ZB - tnow.LL_QAM_ZB)/ tbef.LL_QAM_ZB) > 0.5
        and decode(tbef.LL_GZSL,0,null,null,null, (tbef.LL_GZSL - tnow.LL_GZSL)/ tbef.LL_GZSL) > 0.5
        then 1 else 0 end as LL_TBFLAG,
    case when tbef.CSFB_QUEST_NUM > 10 and tbef.CSFB_CGL - tnow.CSFB_CGL > 5 then 1 else 0 end as CSFB_CGL_TBFLAG,
    case when tbef.TPQH_QUEST_NUM > 100 and tbef.TPQH_CGL - tnow.TPQH_CGL > 10 then 1 else 0 end as TPQH_CGL_TBFLAG,
    case when tbef.TA > 50 and decode(tbef.TA,0,null,null,null, abs(tnow.TA - tbef.TA)/ tbef.TA) > 0.5 then 1 else 0 end as TA_TBFLAG,
    case when tbef.RSRP_ALL_SIMPLES > 500 and tbef.RSRP_110_RATE - tnow.RSRP_110_RATE > 20 then 1 else 0 end as MR_TBFLAG,
    --case when tbef.RSRP_ALL_SIMPLES > 500 and tbef.RSRP_110_RATE - tnow.RSRP_110_RATE > 20 then 1 else 0 end as MR_TBFLAG,
    tnow.LTE_JTL,
    round(tnow.N4_0001*tnow.N4_0003, 2) as LTE_JTL_N,--分子
    round(tnow.N4_0002*tnow.N4_0004, 2) as LTE_JTL_D,--分母
    tnow.LTE_DXL,
    tnow.DXL_DX_NUM as LTE_DXL_N,
    tnow.N4_0006 as LTE_DXL_D,
    tnow.AVG_RTWP,

    tnow.LTE_LL,
    tnow.LL_QAM_ZB,
    tnow.LL_GZSL,

    tnow.CSFB_CGL,
    tnow.N4_0023 as CSFB_CGL_N,
    tnow.CSFB_QUEST_NUM as CSFB_CGL_D,

    tnow.TPQH_CGL,
    tnow.N4_0041 as TPQH_CGL_N,
    tnow.TPQH_QUEST_NUM as TPQH_CGL_D,

    tnow.TA,

    tnow.RSRP_110_RATE,
    tnow.RSRP_ALL_SIMPLES,
    tnow.PROVINCE,
    tnow.VENDOR_CELL_ID,
    tnow.COUNTY,
    tnow.VENDOR_ID,
    tnow.RESERVED3,
    tnow.RESERVED8,
    tnow.town_ID,
    tnow.RNC,
    tnow.RESERVED4,
    tnow.RESERVED5,
    tnow.COVER_TYPE,
    tnow.LIFE,
    tnow.LON,
    tnow.LAT
    from
    (
        select * from TB_LIST_CELL_HOUR t
        --where t.s_date >= to_date(20191003, 'yyyymmdd') and t.s_date <to_date(20191004, 'yyyymmdd') and ecgi = 8397606
        where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
        and s_hour = (case when V_DATE_HOUR is null then s_hour else cast(V_DATE_HOUR as int) end)
    )tnow
    left join
    (
        select * from TB_LIST_CELL_HOUR t
        --where t.s_date >= to_date(20190926, 'yyyymmdd') and t.s_date <to_date(20190927, 'yyyymmdd') and ecgi = 8397606
        where s_date >= to_date(V_DATE_THRESHOLD_START_7_AGO ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END_7_AGO ,'yyyymmdd')
        and s_hour = (case when V_DATE_HOUR is null then s_hour else cast(V_DATE_HOUR as int) end)
    )tbef
    on tnow.ecgi = tbef.ecgi and tnow.s_hour = tbef.s_hour
    order by tnow.ecgi, tnow.s_hour;


    BEGIN
        V_TBNAME := 'TB_LIST_CELL_HOUR_TBZC';

        --索引分区名
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
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);


        if V_DATE_HOUR is not null
          then
              --清理
              execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||') where s_hour='||V_DATE_HOUR into v_clean_flag;
              while v_clean_flag !=0 loop
                  --若入口参数指定了小时，则仅重跑指定小时对应的数据，此处索引至天级分区，并按小时清除
                  execute immediate 'delete from '||v_tbname||' partition('||v_partition_name||') where s_hour='||V_DATE_HOUR;
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
              --清理
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
        -- sql_2 :='select
        -- s_date, s_hour, enb_id, cell_id, ecgi,
        -- lte_jtl, jtl_quest_num,
        -- lte_dxl,dxl_dx_num,
        -- avg_rtwp,
        -- lte_ll, ll_qam_zb, ll_gzsl,
        -- csfb_cgl, csfb_quest_num,
        -- tpqh_cgl, tpqh_quest_num,
        -- ta, rsrp_110_rate, rsrp_100_simples
        -- from TB_LIST_CELL_HOUR partition('||v_partition_name_7_ago||') t where t.ecgi = :ecgi and t.s_hour = :s_hour
        -- ';


        open cur_sql_1; --data_cur
        fetch cur_sql_1 into list_tmp_1;
        loop
            exit when NOT (cur_sql_1%FOUND);
            --标签初始化
            -- JTL_BADFLAG := 0;
            -- DXL_BADFLAG := 0;
            -- AVG_RTWP_BADFLAG := 0;
            -- CSFB_CGL_BADFLAG := 0;
            -- TPQH_CGL_BADFLAG := 0;
            -- MR_BADFLAG := 0;

            JTL_TBFLAG := 0;
            DXL_TBFLAG := 0;
            AVG_RTWP_TBFLAG := 0;
            LL_TBFLAG  := 0;
            CSFB_CGL_TBFLAG := 0;
            TPQH_CGL_TBFLAG := 0;
            TA_TBFLAG :=0;
            MR_TBFLAG := 0;

            --测试语句
            i := i + 1;
            --计算突变天级标签

            --执行插入
            execute immediate 'insert /*+append*/ into TB_LIST_CELL_HOUR_TBZC
            values(
            :S_DATE, :S_HOUR, :ENB_ID, :CELL_ID, :ECGI,
            :JTL_BADFLAG, :DXL_BADFLAG, :AVG_RTWP_BADFLAG, :CSFB_CGL_BADFLAG, :TPQH_CGL_BADFLAG, :MR_BADFLAG,
            :JTL_TBFLAG, :DXL_TBFLAG, :AVG_RTWP_TBFLAG, :LL_TBFLAG, :CSFB_CGL_TBFLAG, :TPQH_CGL_TBFLAG,
            :TA_TBFLAG, :MR_TBFLAG,
            :LTE_JTL, :LTE_JTL_N, :LTE_JTL_D,
            :LTE_DXL, :LTE_DXL_N, :LTE_DXL_D,
            :AVG_RTWP,
            :LTE_LL, :LL_QAM_ZB, :LL_GZSL,
            :CSFB_CGL, :CSFB_CGL_N, :CSFB_CGL_D,
            :TPQH_CGL, :TPQH_CGL_N, :TPQH_CGL_D,
            :TA,
            :RSRP_110_RATE, :RSRP_ALL_SIMPLES,
            :PROVINCE, :VENDOR_CELL_ID, :COUNTY, :VENDOR_ID, :RESERVED3, :RESERVED8,
            :TOWN_ID, :RNC, :RESERVED4, :RESERVED5, :COVER_TYPE, :LIFE, :LON, :LAT)'
            using
            list_tmp_1.S_DATE, list_tmp_1.S_HOUR, list_tmp_1.ENB_ID, list_tmp_1.CELL_ID, list_tmp_1.ECGI,
            list_tmp_1.JTL_BADFLAG, list_tmp_1.DXL_BADFLAG, list_tmp_1.AVG_RTWP_BADFLAG, list_tmp_1.CSFB_CGL_BADFLAG, list_tmp_1.TPQH_CGL_BADFLAG, list_tmp_1.MR_BADFLAG,
            list_tmp_1.JTL_TBFLAG, list_tmp_1.DXL_TBFLAG, list_tmp_1.AVG_RTWP_TBFLAG, list_tmp_1.LL_TBFLAG, list_tmp_1.CSFB_CGL_TBFLAG, list_tmp_1.TPQH_CGL_TBFLAG,
            list_tmp_1.TA_TBFLAG, list_tmp_1.MR_TBFLAG,
            list_tmp_1.LTE_JTL, list_tmp_1.LTE_JTL_N, list_tmp_1.LTE_JTL_D,
            list_tmp_1.LTE_DXL, list_tmp_1.LTE_DXL_N, list_tmp_1.LTE_DXL_D,
            list_tmp_1.AVG_RTWP,
            list_tmp_1.LTE_LL, list_tmp_1.LL_QAM_ZB, list_tmp_1.LL_GZSL,
            list_tmp_1.CSFB_CGL, list_tmp_1.CSFB_CGL_N, list_tmp_1.CSFB_CGL_D,
            list_tmp_1.TPQH_CGL, list_tmp_1.TPQH_CGL_N, list_tmp_1.TPQH_CGL_D,
            list_tmp_1.TA,
            list_tmp_1.RSRP_110_RATE, list_tmp_1.RSRP_ALL_SIMPLES,
            list_tmp_1.PROVINCE, list_tmp_1.VENDOR_CELL_ID, list_tmp_1.COUNTY, list_tmp_1.VENDOR_ID, list_tmp_1.RESERVED3, list_tmp_1.RESERVED8,
            list_tmp_1.TOWN_ID, list_tmp_1.RNC, list_tmp_1.RESERVED4, list_tmp_1.RESERVED5, list_tmp_1.COVER_TYPE, list_tmp_1.LIFE, list_tmp_1.LON, list_tmp_1.LAT;

            if mod(i, 100)=0 --commit every 100 times
                then commit;
            end if;


            --该小区下一小时
            fetch cur_sql_1 into list_tmp_1;



        end loop;
        commit;

        close cur_sql_1;
        dbms_output.put_line('i: '||i);

        --入库数量判断
        execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||')' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        group by s_date, s_hour, ecgi having count(1)>1)'into v_insert_repeat;
        dbms_output.put_line('表 '||v_tbname||' 小时数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');

    END PROC_TB_LIST_CELL_HOUR_TBZC;


-----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
    --in：TB_LIST_CELL_HOUR_TBZC
    --out：TB_INDEX_CELL_DAY
    PROCEDURE PROC_TB_INDEX_CELL_DAY(V_DATE_THRESHOLD_START VARCHAR2) IS --小区小时指标
    --v_high_value_vc                 VARCHAR2(20);
    v_partition_name                VARCHAR2(50);
    --v_partition_name_7_ago          VARCHAR2(50);
    /* v_mr_partition_name             VARCHAR2(50);
    v_mr_partition_name_7_ago       VARCHAR2(50); */
    --V_DATE_THRESHOLD_START_7_AGO    VARCHAR2(8);
    V_TBNAME                        VARCHAR2(100);
    V_DATE_THRESHOLD_END            VARCHAR2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd');--20190927
    sql_2                           VARCHAR2(4000);

    v_insert_cnt   number;
    v_insert_repeat   number;
    v_ssql varchar2(500);
    v_clean_flag number;

    i NUMBER := 0;
    j NUMBER := 0;

    type kpi_3_type is record
    (
      s_date            DATE,
      s_hour            INTEGER,
      enb_id            INTEGER,
      cell_id           INTEGER,
      ecgi              INTEGER,

      JTL_BADFLAG       VARCHAR2(10),
      DXL_BADFLAG       VARCHAR2(10),
      AVG_RTWP_BADFLAG  VARCHAR2(10),
      CSFB_CGL_BADFLAG  VARCHAR2(10),
      TPQH_CGL_BADFLAG  VARCHAR2(10),
      MR_BADFLAG        VARCHAR2(10),
      
      JTL_TBFLAG        VARCHAR2(5),
      DXL_TBFLAG        VARCHAR2(5),
      AVG_RTWP_TBFLAG   VARCHAR2(5),
      LL_TBFLAG         VARCHAR2(5),
      CSFB_CGL_TBFLAG   VARCHAR2(5),
      TPQH_CGL_TBFLAG   VARCHAR2(5),
      TA_TBFLAG         VARCHAR2(5),
      MR_TBFLAG         VARCHAR2(5),
      
      LTE_JTL           NUMBER,
      LTE_JTL_N         NUMBER,
      LTE_JTL_D         NUMBER,

      LTE_DXL           NUMBER,
      LTE_DXL_N         NUMBER,
      LTE_DXL_D         NUMBER,
      AVG_RTWP          NUMBER,

      LTE_LL            NUMBER,
      LL_QAM_ZB         NUMBER,
      LL_GZSL           NUMBER,

      CSFB_CGL          NUMBER,
      CSFB_CGL_N        NUMBER,
      CSFB_CGL_D        NUMBER,
      
      TPQH_CGL          NUMBER,
      TPQH_CGL_N        NUMBER,
      TPQH_CGL_D        NUMBER,
      TA                NUMBER,
      RSRP_110_RATE     NUMBER,
      RSRP_ALL_SIMPLES  NUMBER,
      
      
      province          VARCHAR2(32),
      vendor_cell_id    VARCHAR2(32),
      county            VARCHAR2(32),
      vendor_id         VARCHAR2(32),
      reserved3         VARCHAR2(32),
      reserved8         VARCHAR2(32),
      town_id           VARCHAR2(32),
      rnc               VARCHAR2(32),
      reserved4         VARCHAR2(32),
      reserved5         VARCHAR2(32),
      cover_type        VARCHAR2(32),
      life              VARCHAR2(32),
      lon               NUMBER,
      lat               NUMBER
    );

    kpi_tmp     kpi_3_type;
    kpi_tmp_new kpi_3_type;
    kpi_tmp_old  kpi_3_type;
    
    --质差3临时判别
    JTL_BADFLAG_3_TEMP          VARCHAR2(10) := 0;
    DXL_BADFLAG_3_TEMP          VARCHAR2(10) := 0;
    AVG_RTWP_BADFLAG_3_TEMP     VARCHAR2(10) := 0;
    CSFB_CGL_BADFLAG_3_TEMP     VARCHAR2(10) := 0;
    TPQH_CGL_BADFLAG_3_TEMP     VARCHAR2(10) := 0;
    MR_BADFLAG_3_TEMP           VARCHAR2(10) := 0;
    --质差6临时判别 
    JTL_BADFLAG_6_TEMP          VARCHAR2(10) := 0;
    DXL_BADFLAG_6_TEMP          VARCHAR2(10) := 0;
    AVG_RTWP_BADFLAG_6_TEMP     VARCHAR2(10) := 0;
    CSFB_CGL_BADFLAG_6_TEMP     VARCHAR2(10) := 0;
    TPQH_CGL_BADFLAG_6_TEMP     VARCHAR2(10) := 0;
    MR_BADFLAG_6_TEMP           VARCHAR2(10) := 0;
    
    --质差3结果
    JTL_BADFLAG_3               VARCHAR2(10) := 0;
    DXL_BADFLAG_3               VARCHAR2(10) := 0;
    AVG_RTWP_BADFLAG_3          VARCHAR2(10) := 0;
    CSFB_CGL_BADFLAG_3          VARCHAR2(10) := 0;
    TPQH_CGL_BADFLAG_3          VARCHAR2(10) := 0;
    MR_BADFLAG_3                VARCHAR2(10) := 0;
    --质差6结果
    JTL_BADFLAG_6               VARCHAR2(10) := 0;
    DXL_BADFLAG_6               VARCHAR2(10) := 0;
    AVG_RTWP_BADFLAG_6          VARCHAR2(10) := 0;
    CSFB_CGL_BADFLAG_6          VARCHAR2(10) := 0;
    TPQH_CGL_BADFLAG_6          VARCHAR2(10) := 0;
    MR_BADFLAG_6                VARCHAR2(10) := 0;
    
    --------------
    JTL_TBFLAG_3_TEMP           VARCHAR2(10) := 0;
    DXL_TBFLAG_3_TEMP           VARCHAR2(10) := 0;
    AVG_RTWP_TBFLAG_3_TEMP      VARCHAR2(10) := 0;
    LL_TBFLAG_3_TEMP            VARCHAR2(10) := 0;
    CSFB_CGL_TBFLAG_3_TEMP      VARCHAR2(10) := 0;
    TPQH_CGL_TBFLAG_3_TEMP      VARCHAR2(10) := 0;
    TA_TBFLAG_3_TEMP            VARCHAR2(10) := 0;
    MR_TBFLAG_3_TEMP            VARCHAR2(10) := 0;

    JTL_TBFLAG_6_TEMP           VARCHAR2(10) := 0;
    DXL_TBFLAG_6_TEMP           VARCHAR2(10) := 0;
    AVG_RTWP_TBFLAG_6_TEMP      VARCHAR2(10) := 0;
    LL_TBFLAG_6_TEMP            VARCHAR2(10) := 0;
    CSFB_CGL_TBFLAG_6_TEMP      VARCHAR2(10) := 0;
    TPQH_CGL_TBFLAG_6_TEMP      VARCHAR2(10) := 0;
    TA_TBFLAG_6_TEMP            VARCHAR2(10) := 0;
    MR_TBFLAG_6_TEMP            VARCHAR2(10) := 0;

    JTL_TBFLAG_3                VARCHAR2(10) := 0;
    DXL_TBFLAG_3                VARCHAR2(10) := 0;
    AVG_RTWP_TBFLAG_3           VARCHAR2(10) := 0;
    LL_TBFLAG_3                 VARCHAR2(10) := 0;
    CSFB_CGL_TBFLAG_3           VARCHAR2(10) := 0;
    TPQH_CGL_TBFLAG_3           VARCHAR2(10) := 0;
    TA_TBFLAG_3                 VARCHAR2(10) := 0;
    MR_TBFLAG_3                 VARCHAR2(10) := 0;

    JTL_TBFLAG_6                VARCHAR2(10) := 0;
    DXL_TBFLAG_6                VARCHAR2(10) := 0;
    AVG_RTWP_TBFLAG_6           VARCHAR2(10) := 0;
    LL_TBFLAG_6                 VARCHAR2(10) := 0;
    CSFB_CGL_TBFLAG_6           VARCHAR2(10) := 0;
    TPQH_CGL_TBFLAG_6           VARCHAR2(10) := 0;
    TA_TBFLAG_6                 VARCHAR2(10) := 0;
    MR_TBFLAG_6                 VARCHAR2(10) := 0;
    --------------
    
    --质差&突变终极标签
    JTL_BADFLAG         VARCHAR2(5) := 0;
    DXL_BADFLAG         VARCHAR2(5) := 0;
    AVG_RTWP_BADFLAG    VARCHAR2(5) := 0;
    CSFB_CGL_BADFLAG    VARCHAR2(5) := 0;
    TPQH_CGL_BADFLAG    VARCHAR2(5) := 0;
    MR_BADFLAG          VARCHAR2(5) := 0;
    
    JTL_TBFLAG          VARCHAR2(5) := 0;
    DXL_TBFLAG          VARCHAR2(5) := 0;
    AVG_RTWP_TBFLAG     VARCHAR2(5) := 0;
    LL_TBFLAG           VARCHAR2(5) := 0;
    CSFB_CGL_TBFLAG     VARCHAR2(5) := 0;
    TPQH_CGL_TBFLAG     VARCHAR2(5) := 0;
    TA_TBFLAG           VARCHAR2(5) := 0;
    MR_TBFLAG           VARCHAR2(5) := 0;

    --质差&突变来源标签（来源分类：连续三小时或累计六小时）
    JTL_BADFLAG_SOURCE         VARCHAR2(5) := 0;
    DXL_BADFLAG_SOURCE         VARCHAR2(5) := 0;
    AVG_RTWP_BADFLAG_SOURCE    VARCHAR2(5) := 0;
    CSFB_CGL_BADFLAG_SOURCE    VARCHAR2(5) := 0;
    TPQH_CGL_BADFLAG_SOURCE    VARCHAR2(5) := 0;
    MR_BADFLAG_SOURCE          VARCHAR2(5) := 0;

    JTL_TBFLAG_SOURCE          VARCHAR2(5) := 0;
    DXL_TBFLAG_SOURCE          VARCHAR2(5) := 0;
    AVG_RTWP_TBFLAG_SOURCE     VARCHAR2(5) := 0;
    LL_TBFLAG_SOURCE           VARCHAR2(5) := 0;
    CSFB_CGL_TBFLAG_SOURCE     VARCHAR2(5) := 0;
    TPQH_CGL_TBFLAG_SOURCE     VARCHAR2(5) := 0;
    TA_TBFLAG_SOURCE           VARCHAR2(5) := 0;
    MR_TBFLAG_SOURCE           VARCHAR2(5) := 0;


    /*type partition_type is record(
        table_name      VARCHAR2(200),
        partition_name  VARCHAR2(50),
        high_value      VARCHAR2(100)
    );
    partition_tmp partition_type;

    cursor cur_partition is --字典表内获取非标准分区的分区名
    select table_name,t.partition_name,t.high_value
    from USER_TAB_PARTITIONS t
    where table_name = V_TBNAME and partition_name not like 'P2%'--SYS_21313
    order by to_NUMBER(substr(partition_name,6)) desc;--按照分区名降序遍历*/

    type curtyp is ref cursor;
    cur_sql_2 curtyp;
    
    type end_date_type is record(
        cur_sql_1_end_flag        INTEGER,
        cur_sql_1_hour_end_flag        INTEGER
    );
    end_flag end_date_type;


    --获取单日不同小区号及其附属信息
    cursor cur_sql_1 is
    select * from TB_LIST_CELL_HOUR_TBZC t where t.s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and t.s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
    --and t.s_hour between 9 and 23 
    /*and (ecgi = '8429078' or ecgi = '8429077')*/
    order by ecgi, s_hour;


    BEGIN
        V_TBNAME := 'TB_INDEX_CELL_DAY';

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


        ------------
        V_TBNAME := 'TB_LIST_CELL_HOUR_TBZC';
        --V_DATE_THRESHOLD_START_7_AGO := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd')-7, 'yyyymmdd'); --2019/10/03 - 7 = 2019/09/26 → 2019-09-26
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);

        sql_2 :=
        'select max(ecgi), max(s_hour) from TB_LIST_CELL_HOUR_TBZC partition('||v_partition_name||') t1
        where ecgi in
        (select max(ecgi) ecgi from TB_LIST_CELL_HOUR_TBZC partition('||v_partition_name||') t ) 
        ';
        --where (ecgi = ''8429078'' or ecgi = ''8429077'')

        --execute immediate sql_2 into end_flag;
        open cur_sql_2 for sql_2;
        fetch cur_sql_2 into end_flag;
        loop
            dbms_output.put_line('本次检索的最大ECGI及其对应小时：'||end_flag.cur_sql_1_end_flag||'-'||end_flag.cur_sql_1_hour_end_flag);--当前时间戳对应的小区小时数据
            exit when (end_flag.cur_sql_1_end_flag is not null) or NOT cur_sql_2%FOUND;
        end loop;
        close cur_sql_2;

        open cur_sql_1; --ecgi_cur
        fetch cur_sql_1 into kpi_tmp;
        loop
            exit when NOT cur_sql_1%FOUND;

            /*open cur_sql_2 for sql_2 using omc3_tmp_1.ecgi;--, omc3_tmp_1.s_hour; --data_week_ago
                fetch cur_sql_2 into kpi_tmp;
                loop--------------
                    exit when NOT (cur_sql_2%FOUND);*/
                    --测试语句
            i := i + 1;
            j := j + 1;
            if i = 1 or kpi_tmp.ecgi != kpi_tmp_old.ecgi --本轮开始时，old内仍存着上一轮的数据，故需要重新同步
                then
                    kpi_tmp_new := kpi_tmp;
                    kpi_tmp_old := kpi_tmp;
                else
                    kpi_tmp_old := kpi_tmp_new;
                    kpi_tmp_new := kpi_tmp;
                    --追踪
                    /*if kpi_tmp.ecgi = 10074898 
                       then 
                          dbms_output.put_line(kpi_tmp_old.ecgi||'-'||to_char(kpi_tmp_old.s_date, 'yyyymmdd')||'-'||kpi_tmp_old.s_hour);
                          dbms_output.put_line(kpi_tmp_new.ecgi||'-'||to_char(kpi_tmp_new.s_date, 'yyyymmdd')||'-'||kpi_tmp_new.s_hour);
                          dbms_output.put_line(kpi_tmp_old.s_hour||':'||kpi_tmp_old.MR_TBFLAG||'-'||kpi_tmp_new.s_hour||':'||kpi_tmp_new.MR_TBFLAG);
                          dbms_output.put_line(MR_TBFLAG_3_TEMP||'-'||MR_TBFLAG_6_TEMP);
                          dbms_output.put_line(MR_TBFLAG_3||'-'||MR_TBFLAG_6);
                    end if;*/
            end if;
            --dbms_output.put_line(kpi_tmp_new.ecgi||'-'||to_char(kpi_tmp_new.s_date, 'yyyymmdd')||'-'||kpi_tmp_new.s_hour);--当前时间戳对应的小区小时数据
            --dbms_output.put_line(kpi_tmp_old.ecgi||'-'||to_char(kpi_tmp_old.s_date, 'yyyymmdd')||'-'||kpi_tmp_old.s_hour);--当前时间戳对应的小区小时数据

            --开始进行小区小时级标签判断（质差&突变连续性标签：3&6）
            --1、质差连续或累计判断：某小区某小时
            if i != 1 and (kpi_tmp_new.JTL_BADFLAG = 1 and kpi_tmp_old.JTL_BADFLAG = 1)
                then JTL_BADFLAG_3_TEMP := JTL_BADFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.DXL_BADFLAG = 1 and kpi_tmp_old.DXL_BADFLAG = 1)
                then DXL_BADFLAG_3_TEMP := DXL_BADFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.AVG_RTWP_BADFLAG = 1 and kpi_tmp_old.AVG_RTWP_BADFLAG = 1)
                then AVG_RTWP_BADFLAG_3_TEMP := AVG_RTWP_BADFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.CSFB_CGL_BADFLAG = 1 and kpi_tmp_old.CSFB_CGL_BADFLAG = 1)
                then CSFB_CGL_BADFLAG_3_TEMP := CSFB_CGL_BADFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.TPQH_CGL_BADFLAG = 1 and kpi_tmp_old.TPQH_CGL_BADFLAG = 1)
                then TPQH_CGL_BADFLAG_3_TEMP := TPQH_CGL_BADFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.MR_BADFLAG = 1 and kpi_tmp_old.MR_BADFLAG = 1)
                then MR_BADFLAG_3_TEMP := MR_BADFLAG_3_TEMP + 1;
            end if;

            if kpi_tmp.JTL_BADFLAG = 1 then JTL_BADFLAG_6_TEMP := JTL_BADFLAG_6_TEMP + 1; 
            end if;
            if kpi_tmp.DXL_BADFLAG = 1 then DXL_BADFLAG_6_TEMP := DXL_BADFLAG_6_TEMP + 1; 
            end if;
            if kpi_tmp.AVG_RTWP_BADFLAG = 1 then AVG_RTWP_BADFLAG_6_TEMP := AVG_RTWP_BADFLAG_6_TEMP + 1; 
            end if;
            if kpi_tmp.CSFB_CGL_BADFLAG = 1 then CSFB_CGL_BADFLAG_6_TEMP := CSFB_CGL_BADFLAG_6_TEMP + 1; 
            end if;
            if kpi_tmp.TPQH_CGL_BADFLAG = 1 then TPQH_CGL_BADFLAG_6_TEMP := TPQH_CGL_BADFLAG_6_TEMP + 1; 
            end if;  
            if kpi_tmp.MR_BADFLAG = 1 then MR_BADFLAG_6_TEMP := MR_BADFLAG_6_TEMP + 1; 
            end if;

            --2、该小区突变连续或累计判断
            if i != 1 and (kpi_tmp_new.JTL_TBFLAG = 1 and kpi_tmp_old.JTL_TBFLAG = 1)
                then JTL_TBFLAG_3_TEMP := JTL_TBFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.DXL_TBFLAG = 1 and kpi_tmp_old.DXL_TBFLAG = 1)
                then DXL_TBFLAG_3_TEMP := DXL_TBFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.AVG_RTWP_TBFLAG = 1 and kpi_tmp_old.AVG_RTWP_TBFLAG = 1)
                then AVG_RTWP_TBFLAG_3_TEMP := AVG_RTWP_TBFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.LL_TBFLAG = 1 and kpi_tmp_old.LL_TBFLAG = 1)
                then LL_TBFLAG_3_TEMP := LL_TBFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.CSFB_CGL_TBFLAG = 1 and kpi_tmp_old.CSFB_CGL_TBFLAG = 1)
                then CSFB_CGL_TBFLAG_3_TEMP := CSFB_CGL_TBFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.TPQH_CGL_TBFLAG = 1 and kpi_tmp_old.TPQH_CGL_TBFLAG = 1)
                then TPQH_CGL_TBFLAG_3_TEMP := TPQH_CGL_TBFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.TA_TBFLAG = 1 and kpi_tmp_old.TA_TBFLAG = 1)
                then TA_TBFLAG_3_TEMP := TA_TBFLAG_3_TEMP + 1;
            end if;
            if i != 1 and (kpi_tmp_new.MR_TBFLAG = 1 and kpi_tmp_old.MR_TBFLAG = 1)
                then MR_TBFLAG_3_TEMP := MR_TBFLAG_3_TEMP + 1;
            end if;
            
            if kpi_tmp.JTL_TBFLAG = 1 then JTL_TBFLAG_6_TEMP := JTL_TBFLAG_6_TEMP + 1; 
            end if;
            if kpi_tmp.DXL_TBFLAG = 1 then DXL_TBFLAG_6_TEMP := DXL_TBFLAG_6_TEMP + 1; 
            end if;
            if kpi_tmp.AVG_RTWP_TBFLAG = 1 then AVG_RTWP_TBFLAG_6_TEMP := AVG_RTWP_TBFLAG_6_TEMP + 1; 
            end if;
            if kpi_tmp.LL_TBFLAG = 1 then LL_TBFLAG_6_TEMP := LL_TBFLAG_6_TEMP + 1; 
            end if;
            if kpi_tmp.CSFB_CGL_TBFLAG = 1 then CSFB_CGL_TBFLAG_6_TEMP := CSFB_CGL_TBFLAG_6_TEMP + 1; 
            end if;
            if kpi_tmp.TPQH_CGL_TBFLAG = 1 then TPQH_CGL_TBFLAG_6_TEMP := TPQH_CGL_TBFLAG_6_TEMP + 1; 
            end if;  
            if kpi_tmp.TA_TBFLAG = 1 then TA_TBFLAG_6_TEMP := TA_TBFLAG_6_TEMP + 1;
            end if;
            if kpi_tmp.MR_TBFLAG = 1 then MR_TBFLAG_6_TEMP := MR_TBFLAG_6_TEMP + 1; 
            end if;
            

            --该小区下一小时
            fetch cur_sql_1 into kpi_tmp;
            
            --小区切换或到达最终要遍历的ecgi
            if kpi_tmp.ecgi != kpi_tmp_old.ecgi or (kpi_tmp_new.ecgi = end_flag.cur_sql_1_end_flag and kpi_tmp_new.s_hour = end_flag.cur_sql_1_hour_end_flag)
            --游动至另一个小区时：1、判断上一小区连续三小时或合计六小时标签；2、Commit；3、标志位重置.
                then
                    --质差：连续三小时判断
                    if JTL_BADFLAG_3_TEMP >= 2 then JTL_BADFLAG_3 := 1;
                    end if;
                    if DXL_BADFLAG_3_TEMP >= 2 then DXL_BADFLAG_3 := 1;
                    end if;
                    if AVG_RTWP_BADFLAG_3_TEMP >= 2 then AVG_RTWP_BADFLAG_3 := 1;
                    end if;
                    if CSFB_CGL_BADFLAG_3_TEMP >= 2 then CSFB_CGL_BADFLAG_3 := 1;
                    end if;
                    if TPQH_CGL_BADFLAG_3_TEMP >= 2 then TPQH_CGL_BADFLAG_3 := 1;
                    end if;
                    if MR_BADFLAG_3_TEMP >= 2 then MR_BADFLAG_3 := 1;
                    end if;
                    
                    --质差：累计六次判断
                    if JTL_BADFLAG_6_TEMP >= 6 then JTL_BADFLAG_6 := 1;
                    end if;
                    if DXL_BADFLAG_6_TEMP >= 6 then DXL_BADFLAG_6 := 1;
                    end if;
                    if AVG_RTWP_BADFLAG_6_TEMP >= 6 then AVG_RTWP_BADFLAG_6 := 1;
                    end if;
                    if CSFB_CGL_BADFLAG_6_TEMP >= 6 then CSFB_CGL_BADFLAG_6 := 1;
                    end if;
                    if TPQH_CGL_BADFLAG_6_TEMP >= 6 then TPQH_CGL_BADFLAG_6 := 1;
                    end if;
                    if MR_BADFLAG_6_TEMP >= 6 then MR_BADFLAG_6 := 1;
                    end if;

                    --突变：连续三小时判断
                    if JTL_TBFLAG_3_TEMP >= 2 then JTL_TBFLAG_3 := 1;
                    end if;
                    if DXL_TBFLAG_3_TEMP >= 2 then DXL_TBFLAG_3 := 1;
                    end if;
                    if AVG_RTWP_TBFLAG_3_TEMP >= 2 then AVG_RTWP_TBFLAG_3 := 1;
                    end if;
                    if LL_TBFLAG_3_TEMP >= 2 then LL_TBFLAG_3 := 1;
                    end if;
                    if CSFB_CGL_TBFLAG_3_TEMP >= 2 then CSFB_CGL_TBFLAG_3 := 1;
                    end if;
                    if TPQH_CGL_TBFLAG_3_TEMP >= 2 then TPQH_CGL_TBFLAG_3 := 1;
                    end if;
                    if TA_TBFLAG_3_TEMP >= 2 then TA_TBFLAG_3 := 1;
                    end if;
                    if MR_TBFLAG_3_TEMP >= 2 then MR_TBFLAG_3 := 1;
                    end if;
                    
                    --突变：累计六次判断
                    if JTL_TBFLAG_6_TEMP >= 6 then JTL_TBFLAG_6 := 1;
                    end if;
                    if DXL_TBFLAG_6_TEMP >= 6 then DXL_TBFLAG_6 := 1;
                    end if;
                    if AVG_RTWP_TBFLAG_6_TEMP >= 6 then AVG_RTWP_TBFLAG_6 := 1;
                    end if;
                    if LL_TBFLAG_6_TEMP >= 6 then LL_TBFLAG_6 := 1;
                    end if;
                    if CSFB_CGL_TBFLAG_6_TEMP >= 6 then CSFB_CGL_TBFLAG_6 := 1;
                    end if;
                    if TPQH_CGL_TBFLAG_6_TEMP >= 6 then TPQH_CGL_TBFLAG_6 := 1;
                    end if;
                    if TA_TBFLAG_6_TEMP >= 6 then TA_TBFLAG_6 := 1;
                    end if;
                    if MR_TBFLAG_6_TEMP >= 6 then MR_TBFLAG_6 := 1;
                    end if;
                    
                    /*if kpi_tmp_old.ecgi = '8429078' then
                       dbms_output.put_line(kpi_tmp_new.ecgi||'-'||LL_TBFLAG||'-'||LL_TBFLAG_3||'-'||LL_TBFLAG_6_TEMP||'-'||LL_TBFLAG_6);
                    end if;*/
                    
                    --3、最终质差&突变标签判定
                    --质差终极标签
                    if JTL_BADFLAG_3 = 1 and JTL_BADFLAG_6 = 1
                        then 
                            JTL_BADFLAG := 1; 
                            JTL_BADFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif JTL_BADFLAG_3 = 1 and JTL_BADFLAG_6 = 0
                        then
                            JTL_BADFLAG := 1; 
                            JTL_BADFLAG_SOURCE := '3';
                    elsif JTL_BADFLAG_3 = 0 and JTL_BADFLAG_6 = 1
                        then 
                            JTL_BADFLAG := 1;
                            JTL_BADFLAG_SOURCE := '6';
                    else    JTL_BADFLAG := 0;
                            JTL_BADFLAG_SOURCE := '0';
                    end if;
                    

                    if DXL_BADFLAG_3 = 1 and DXL_BADFLAG_6 = 1
                        then 
                            DXL_BADFLAG := 1; 
                            DXL_BADFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif DXL_BADFLAG_3 = 1 and DXL_BADFLAG_6 = 0
                        then
                            DXL_BADFLAG := 1; 
                            DXL_BADFLAG_SOURCE := '3';
                    elsif DXL_BADFLAG_3 = 0 and DXL_BADFLAG_6 = 1
                        then 
                            DXL_BADFLAG := 1;
                            DXL_BADFLAG_SOURCE := '6';
                    else    DXL_BADFLAG := 0;
                            DXL_BADFLAG_SOURCE := '0';
                    end if;


                    if AVG_RTWP_BADFLAG_3 = 1 and AVG_RTWP_BADFLAG_6 = 1
                        then 
                            AVG_RTWP_BADFLAG := 1; 
                            AVG_RTWP_BADFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif AVG_RTWP_BADFLAG_3 = 1 and AVG_RTWP_BADFLAG_6 = 0
                        then
                            AVG_RTWP_BADFLAG := 1; 
                            AVG_RTWP_BADFLAG_SOURCE := '3';
                    elsif AVG_RTWP_BADFLAG_3 = 0 and AVG_RTWP_BADFLAG_6 = 1
                        then 
                            AVG_RTWP_BADFLAG := 1;
                            AVG_RTWP_BADFLAG_SOURCE := '6';
                    else    AVG_RTWP_BADFLAG := 0;
                            AVG_RTWP_BADFLAG_SOURCE := '0';
                    end if;


                    if CSFB_CGL_BADFLAG_3 = 1 and CSFB_CGL_BADFLAG_6 = 1
                        then 
                            CSFB_CGL_BADFLAG := 1; 
                            CSFB_CGL_BADFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif CSFB_CGL_BADFLAG_3 = 1 and CSFB_CGL_BADFLAG_6 = 0
                        then
                            CSFB_CGL_BADFLAG := 1; 
                            CSFB_CGL_BADFLAG_SOURCE := '3';
                    elsif CSFB_CGL_BADFLAG_3 = 0 and CSFB_CGL_BADFLAG_6 = 1
                        then 
                            CSFB_CGL_BADFLAG := 1;
                            CSFB_CGL_BADFLAG_SOURCE := '6';
                    else    CSFB_CGL_BADFLAG := 0;
                            CSFB_CGL_BADFLAG_SOURCE := '0';
                    end if;           


                    if TPQH_CGL_BADFLAG_3 = 1 and TPQH_CGL_BADFLAG_6 = 1
                        then 
                            TPQH_CGL_BADFLAG := 1; 
                            TPQH_CGL_BADFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif TPQH_CGL_BADFLAG_3 = 1 and TPQH_CGL_BADFLAG_6 = 0
                        then
                            TPQH_CGL_BADFLAG := 1; 
                            TPQH_CGL_BADFLAG_SOURCE := '3';
                    elsif TPQH_CGL_BADFLAG_3 = 0 and TPQH_CGL_BADFLAG_6 = 1
                        then 
                            TPQH_CGL_BADFLAG := 1;
                            TPQH_CGL_BADFLAG_SOURCE := '6';
                    else    TPQH_CGL_BADFLAG := 0;
                            TPQH_CGL_BADFLAG_SOURCE := '0';
                    end if;


                    if MR_BADFLAG_3 = 1 and MR_BADFLAG_6 = 1
                        then 
                            MR_BADFLAG := 1; 
                            MR_BADFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif MR_BADFLAG_3 = 1 and MR_BADFLAG_6 = 0
                        then
                            MR_BADFLAG := 1; 
                            MR_BADFLAG_SOURCE := '3';
                    elsif MR_BADFLAG_3 = 0 and MR_BADFLAG_6 = 1
                        then 
                            MR_BADFLAG := 1;
                            MR_BADFLAG_SOURCE := '6';
                    else    MR_BADFLAG := 0;
                            MR_BADFLAG_SOURCE := '0';
                    end if;


                    --突变终极标签
                    if JTL_TBFLAG_3 = 1 and JTL_TBFLAG_6 = 1
                        then 
                            JTL_TBFLAG := 1; 
                            JTL_TBFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif JTL_TBFLAG_3 = 1 and JTL_TBFLAG_6 = 0
                        then
                            JTL_TBFLAG := 1; 
                            JTL_TBFLAG_SOURCE := '3';
                    elsif JTL_TBFLAG_3 = 0 and JTL_TBFLAG_6 = 1
                        then 
                            JTL_TBFLAG := 1;
                            JTL_TBFLAG_SOURCE := '6';
                    else    JTL_TBFLAG := 0;
                            JTL_TBFLAG_SOURCE := '0';
                    end if;
                    

                    if DXL_TBFLAG_3 = 1 and DXL_TBFLAG_6 = 1
                        then 
                            DXL_TBFLAG := 1; 
                            DXL_TBFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif DXL_TBFLAG_3 = 1 and DXL_TBFLAG_6 = 0
                        then
                            DXL_TBFLAG := 1; 
                            DXL_TBFLAG_SOURCE := '3';
                    elsif DXL_TBFLAG_3 = 0 and DXL_TBFLAG_6 = 1
                        then 
                            DXL_TBFLAG := 1;
                            DXL_TBFLAG_SOURCE := '6';
                    else    DXL_TBFLAG := 0;
                            DXL_TBFLAG_SOURCE := '0';
                    end if;


                    if AVG_RTWP_TBFLAG_3 = 1 and AVG_RTWP_TBFLAG_6 = 1
                        then 
                            AVG_RTWP_TBFLAG := 1; 
                            AVG_RTWP_TBFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif AVG_RTWP_TBFLAG_3 = 1 and AVG_RTWP_TBFLAG_6 = 0
                        then
                            AVG_RTWP_TBFLAG := 1; 
                            AVG_RTWP_TBFLAG_SOURCE := '3';
                    elsif AVG_RTWP_TBFLAG_3 = 0 and AVG_RTWP_TBFLAG_6 = 1
                        then 
                            AVG_RTWP_TBFLAG := 1;
                            AVG_RTWP_TBFLAG_SOURCE := '6';
                    else    AVG_RTWP_TBFLAG := 0;
                            AVG_RTWP_TBFLAG_SOURCE := '0';
                    end if;

                    if LL_TBFLAG_3 = 1 and LL_TBFLAG_6 = 1
                        then 
                            LL_TBFLAG := 1; 
                            LL_TBFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif LL_TBFLAG_3 = 1 and LL_TBFLAG_6 = 0
                        then
                            LL_TBFLAG := 1; 
                            LL_TBFLAG_SOURCE := '3';
                    elsif LL_TBFLAG_3 = 0 and LL_TBFLAG_6 = 1
                        then 
                            LL_TBFLAG := 1;
                            LL_TBFLAG_SOURCE := '6';
                    else    LL_TBFLAG := 0;
                            LL_TBFLAG_SOURCE := '0';
                    end if;


                    if CSFB_CGL_TBFLAG_3 = 1 and CSFB_CGL_TBFLAG_6 = 1
                        then 
                            CSFB_CGL_TBFLAG := 1; 
                            CSFB_CGL_TBFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif CSFB_CGL_TBFLAG_3 = 1 and CSFB_CGL_TBFLAG_6 = 0
                        then
                            CSFB_CGL_TBFLAG := 1; 
                            CSFB_CGL_TBFLAG_SOURCE := '3';
                    elsif CSFB_CGL_TBFLAG_3 = 0 and CSFB_CGL_TBFLAG_6 = 1
                        then 
                            CSFB_CGL_TBFLAG := 1;
                            CSFB_CGL_TBFLAG_SOURCE := '6';
                    else    CSFB_CGL_TBFLAG := 0;
                            CSFB_CGL_TBFLAG_SOURCE := '0';
                    end if;           


                    if TPQH_CGL_TBFLAG_3 = 1 and TPQH_CGL_TBFLAG_6 = 1
                        then 
                            TPQH_CGL_TBFLAG := 1; 
                            TPQH_CGL_TBFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif TPQH_CGL_TBFLAG_3 = 1 and TPQH_CGL_TBFLAG_6 = 0
                        then
                            TPQH_CGL_TBFLAG := 1; 
                            TPQH_CGL_TBFLAG_SOURCE := '3';
                    elsif TPQH_CGL_TBFLAG_3 = 0 and TPQH_CGL_TBFLAG_6 = 1
                        then 
                            TPQH_CGL_TBFLAG := 1;
                            TPQH_CGL_TBFLAG_SOURCE := '6';
                    else    TPQH_CGL_TBFLAG := 0;
                            TPQH_CGL_TBFLAG_SOURCE := '0';
                    end if;


                    if TA_TBFLAG_3 = 1 and TA_TBFLAG_6 = 1
                        then 
                            TA_TBFLAG := 1; 
                            TA_TBFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif TA_TBFLAG_3 = 1 and TA_TBFLAG_6 = 0
                        then
                            TA_TBFLAG := 1; 
                            TA_TBFLAG_SOURCE := '3';
                    elsif TA_TBFLAG_3 = 0 and TA_TBFLAG_6 = 1
                        then 
                            TA_TBFLAG := 1;
                            TA_TBFLAG_SOURCE := '6';
                    else    TA_TBFLAG := 0;
                            TA_TBFLAG_SOURCE := '0';
                    end if;


                    if MR_TBFLAG_3 = 1 and MR_TBFLAG_6 = 1
                        then 
                            MR_TBFLAG := 1; 
                            MR_TBFLAG_SOURCE := '3-6'; --添加标签来源
                    elsif MR_TBFLAG_3 = 1 and MR_TBFLAG_6 = 0
                        then
                            MR_TBFLAG := 1; 
                            MR_TBFLAG_SOURCE := '3';
                    elsif MR_TBFLAG_3 = 0 and MR_TBFLAG_6 = 1
                        then 
                            MR_TBFLAG := 1;
                            MR_TBFLAG_SOURCE := '6';
                    else    MR_TBFLAG := 0;
                            MR_TBFLAG_SOURCE := '0';
                    end if;


                    execute immediate 'insert /*+append*/ into TB_INDEX_CELL_DAY
                    values(
                    :s_date, :enb_id, :cell_id, :ecgi,

                    :JTL_BADFLAG,
                    :DXL_BADFLAG,
                    :AVG_RTWP_BADFLAG,
                    :CSFB_CGL_BADFLAG,
                    :TPQH_CGL_BADFLAG,
                    :MR_BADFLAG,

                    :JTL_TBFLAG,
                    :DXL_TBFLAG,
                    :AVG_RTWP_TBFLAG,
                    :LL_TBFLAG,
                    :CSFB_CGL_TBFLAG,
                    :TPQH_CGL_TBFLAG,
                    :TA_TBFLAG,
                    :MR_TBFLAG,

                    :JTL_BADFLAG_SOURCE,
                    :DXL_BADFLAG_SOURCE,
                    :AVG_RTWP_BADFLAG_SOURCE,
                    :CSFB_CGL_BADFLAG_SOURCE,
                    :TPQH_CGL_BADFLAG_SOURCE,
                    :MR_BADFLAG_SOURCE,

                    :JTL_TBFLAG_SOURCE,
                    :DXL_TBFLAG_SOURCE,
                    :AVG_RTWP_TBFLAG_SOURCE,
                    :LL_TBFLAG_SOURCE,
                    :CSFB_CGL_TBFLAG_SOURCE,
                    :TPQH_CGL_TBFLAG_SOURCE,
                    :TA_TBFLAG_SOURCE,
                    :MR_TBFLAG_SOURCE,

                    :JTL_BADFLAG_3,
                    :DXL_BADFLAG_3,
                    :AVG_RTWP_BADFLAG_3,
                    :CSFB_CGL_BADFLAG_3,
                    :TPQH_CGL_BADFLAG_3,
                    :MR_BADFLAG_3,
                    :JTL_BADFLAG_6,
                    :DXL_BADFLAG_6,
                    :AVG_RTWP_BADFLAG_6,
                    :CSFB_CGL_BADFLAG_6,
                    :TPQH_CGL_BADFLAG_6,
                    :MR_BADFLAG_6,
                    
                    :JTL_TBFLAG_3,
                    :DXL_TBFLAG_3,
                    :AVG_RTWP_TBFLAG_3,
                    :LL_TBFLAG_3,
                    :CSFB_CGL_TBFLAG_3,
                    :TPQH_CGL_TBFLAG_3,
                    :TA_TBFLAG_3,
                    :MR_TBFLAG_3,

                    :JTL_TBFLAG_6,
                    :DXL_TBFLAG_6,
                    :AVG_RTWP_TBFLAG_6,
                    :LL_TBFLAG_6,
                    :CSFB_CGL_TBFLAG_6,
                    :TPQH_CGL_TBFLAG_6,
                    :TA_TBFLAG_6,
                    :MR_TBFLAG_6,
                    
                    :PROVINCE, :VENDOR_CELL_ID, :COUNTY, :VENDOR_ID, :RESERVED3, :RESERVED8,
                    :TOWN_ID, :RNC, :RESERVED4, :RESERVED5, :COVER_TYPE, :LIFE, :LON, :LAT)'
                    using
                    kpi_tmp_old.s_date, kpi_tmp_old.enb_id, kpi_tmp_old.cell_id, kpi_tmp_old.ecgi,

                    JTL_BADFLAG,
                    DXL_BADFLAG,
                    AVG_RTWP_BADFLAG,
                    CSFB_CGL_BADFLAG,
                    TPQH_CGL_BADFLAG,
                    MR_BADFLAG,

                    JTL_TBFLAG,
                    DXL_TBFLAG,
                    AVG_RTWP_TBFLAG,
                    LL_TBFLAG,
                    CSFB_CGL_TBFLAG,
                    TPQH_CGL_TBFLAG,
                    TA_TBFLAG,
                    MR_TBFLAG,

                    JTL_BADFLAG_SOURCE,
                    DXL_BADFLAG_SOURCE,
                    AVG_RTWP_BADFLAG_SOURCE,
                    CSFB_CGL_BADFLAG_SOURCE,
                    TPQH_CGL_BADFLAG_SOURCE,
                    MR_BADFLAG_SOURCE,

                    JTL_TBFLAG_SOURCE,
                    DXL_TBFLAG_SOURCE,
                    AVG_RTWP_TBFLAG_SOURCE,
                    LL_TBFLAG_SOURCE,
                    CSFB_CGL_TBFLAG_SOURCE,
                    TPQH_CGL_TBFLAG_SOURCE,
                    TA_TBFLAG_SOURCE,
                    MR_TBFLAG_SOURCE,
                    
                    JTL_BADFLAG_3, DXL_BADFLAG_3,
                    AVG_RTWP_BADFLAG_3, CSFB_CGL_BADFLAG_3,
                    TPQH_CGL_BADFLAG_3,
                    MR_BADFLAG_3,
                    JTL_BADFLAG_6, DXL_BADFLAG_6,
                    AVG_RTWP_BADFLAG_6, CSFB_CGL_BADFLAG_6,
                    TPQH_CGL_BADFLAG_6,
                    MR_BADFLAG_6,
                    
                    JTL_TBFLAG_3,
                    DXL_TBFLAG_3,
                    AVG_RTWP_TBFLAG_3,
                    LL_TBFLAG_3,
                    CSFB_CGL_TBFLAG_3,
                    TPQH_CGL_TBFLAG_3,
                    TA_TBFLAG_3,
                    MR_TBFLAG_3,
                    
                    JTL_TBFLAG_6,
                    DXL_TBFLAG_6,
                    AVG_RTWP_TBFLAG_6,
                    LL_TBFLAG_6,
                    CSFB_CGL_TBFLAG_6,
                    TPQH_CGL_TBFLAG_6,
                    TA_TBFLAG_6,
                    MR_TBFLAG_6,
                    
                    kpi_tmp_old.PROVINCE, kpi_tmp_old.VENDOR_CELL_ID, kpi_tmp_old.COUNTY,
                    kpi_tmp_old.VENDOR_ID, kpi_tmp_old.RESERVED3, kpi_tmp_old.RESERVED8,
                    kpi_tmp_old.TOWN_ID, kpi_tmp_old.RNC, kpi_tmp_old.RESERVED4, kpi_tmp_old.RESERVED5, kpi_tmp_old.COVER_TYPE,
                    kpi_tmp_old.LIFE, kpi_tmp_old.LON, kpi_tmp_old.LAT;

                    if mod(j, 200)=0 --commit every 200 times
                        then commit;
                    end if;
                    
                    --标签重置
                    --循环标签重置！！！--若丢失该标签，会导致 INDEX 表每个小区第一次判定重复，即9/9时，就进行了第一次标签判定，正确应是9/10时才开始
                    i := 0;
                    
                    --质差标签重置
                    
                    JTL_BADFLAG_3_TEMP := 0;
                    DXL_BADFLAG_3_TEMP := 0;
                    AVG_RTWP_BADFLAG_3_TEMP  := 0;
                    CSFB_CGL_BADFLAG_3_TEMP := 0;
                    TPQH_CGL_BADFLAG_3_TEMP := 0;
                    MR_BADFLAG_3_TEMP := 0;

                    JTL_BADFLAG_3 := 0;
                    DXL_BADFLAG_3 := 0;
                    AVG_RTWP_BADFLAG_3  := 0;
                    CSFB_CGL_BADFLAG_3 := 0;
                    TPQH_CGL_BADFLAG_3 := 0;
                    MR_BADFLAG_3 := 0;
                    
                    JTL_BADFLAG_6_TEMP := 0;
                    DXL_BADFLAG_6_TEMP := 0;
                    AVG_RTWP_BADFLAG_6_TEMP  := 0;
                    CSFB_CGL_BADFLAG_6_TEMP := 0;
                    TPQH_CGL_BADFLAG_6_TEMP := 0;
                    MR_BADFLAG_6_TEMP := 0;                    
                    
                    JTL_BADFLAG_6 := 0;
                    DXL_BADFLAG_6 := 0;
                    AVG_RTWP_BADFLAG_6  := 0;
                    CSFB_CGL_BADFLAG_6 := 0;
                    TPQH_CGL_BADFLAG_6 := 0;
                    MR_BADFLAG_6 := 0;
                    
                    --突变标签重置
                    JTL_TBFLAG_3_TEMP := 0;
                    DXL_TBFLAG_3_TEMP := 0;
                    AVG_RTWP_TBFLAG_3_TEMP  := 0;
                    LL_TBFLAG_3_TEMP := 0;
                    CSFB_CGL_TBFLAG_3_TEMP := 0;
                    TPQH_CGL_TBFLAG_3_TEMP := 0;
                    TA_TBFLAG_3_TEMP := 0;
                    MR_TBFLAG_3_TEMP := 0;

                    JTL_TBFLAG_3 := 0;
                    DXL_TBFLAG_3 := 0;
                    AVG_RTWP_TBFLAG_3  := 0;
                    LL_TBFLAG_3 := 0;
                    CSFB_CGL_TBFLAG_3 := 0;
                    TPQH_CGL_TBFLAG_3 := 0;
                    TA_TBFLAG_3 := 0;
                    MR_TBFLAG_3 := 0;
                    
                    JTL_TBFLAG_6_TEMP := 0;
                    DXL_TBFLAG_6_TEMP := 0;
                    AVG_RTWP_TBFLAG_6_TEMP  := 0;
                    LL_TBFLAG_6_TEMP := 0;
                    CSFB_CGL_TBFLAG_6_TEMP := 0;
                    TPQH_CGL_TBFLAG_6_TEMP := 0;
                    TA_TBFLAG_6_TEMP := 0;
                    MR_TBFLAG_6_TEMP := 0;                    
                    
                    JTL_TBFLAG_6 := 0;
                    DXL_TBFLAG_6 := 0;
                    AVG_RTWP_TBFLAG_6  := 0;
                    LL_TBFLAG_6 := 0;
                    CSFB_CGL_TBFLAG_6 := 0;
                    TPQH_CGL_TBFLAG_6 := 0;
                    TA_TBFLAG_6 := 0;
                    MR_TBFLAG_6 := 0;
                    
                    --终极标签重置
                    JTL_BADFLAG          := 0;
                    DXL_BADFLAG          := 0;
                    AVG_RTWP_BADFLAG     := 0;
                    CSFB_CGL_BADFLAG     := 0;
                    TPQH_CGL_BADFLAG    := 0;
                    MR_BADFLAG           := 0;
                    
                    JTL_TBFLAG          := 0;
                    DXL_TBFLAG           := 0;
                    AVG_RTWP_TBFLAG      := 0;
                    LL_TBFLAG            := 0;
                    CSFB_CGL_TBFLAG      := 0;
                    TPQH_CGL_TBFLAG      := 0;
                    TA_TBFLAG            := 0;
                    MR_TBFLAG            := 0;

                    --终极标签来源重置
                    JTL_BADFLAG_SOURCE          := 0;
                    DXL_BADFLAG_SOURCE          := 0;
                    AVG_RTWP_BADFLAG_SOURCE     := 0;
                    CSFB_CGL_BADFLAG_SOURCE     := 0;
                    TPQH_CGL_BADFLAG_SOURCE     := 0;
                    MR_BADFLAG_SOURCE           := 0;

                    JTL_TBFLAG_SOURCE           := 0;
                    DXL_TBFLAG_SOURCE           := 0;
                    AVG_RTWP_TBFLAG_SOURCE      := 0;
                    LL_TBFLAG_SOURCE            := 0;
                    CSFB_CGL_TBFLAG_SOURCE      := 0;
                    TPQH_CGL_TBFLAG_SOURCE     := 0;
                    TA_TBFLAG_SOURCE            := 0;
                    MR_TBFLAG_SOURCE           := 0;
                    
                    
                    ------------
            end if;
        end loop;

        commit;
        close cur_sql_1;
        dbms_output.put_line('j: '||j);


        V_TBNAME := 'TB_INDEX_CELL_DAY';
        --入库数量判断
        execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||')' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        group by s_date, ecgi having count(1)>1)'into v_insert_repeat;
        dbms_output.put_line('表 '||v_tbname||' 天级数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');

    END PROC_TB_INDEX_CELL_DAY;


-----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
    --in：TB_INDEX_CELL_DAY / DT_CELL_L
    --out：OSS_CELL_NUM
    PROCEDURE PROC_TB_OSS_DAY(V_DATE_THRESHOLD_START VARCHAR2) IS
    V_TBNAME                    varchar2(100);
    --V_DATE_THRESHOLD_END            varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd'); --20191004
    v_partition_name_clean      varchar2(50);
    v_partition_name            varchar2(50);
    --v_high_value_vc             varchar2(20);
    
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
    
    /*type partition_type is record
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
    order by to_number(substr(partition_name,6)) desc;--按照分区名降序遍历*/
    
    --cursor cur_sql_2 is
    SQL_1 clob;
    SQL_2 clob;
    TYPE CurTyp IS REF CURSOR;
    
    CUR_SQL_1	CurTyp;
    
    j number := 0;

    BEGIN
       
       --程序超时判定的起始时间设置
        v_timeout := sysdate;
        
        V_TBNAME := 'OSS_CELL_NUM';
        --索引分区名
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME_CLEAN);
        /*open cur_partition; --开始索引字典表
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
        close cur_partition;--------------*/
        
        --清理
        if v_partition_name_clean <> 'NULL'
            then
                execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name_clean||') where net_type = ''4G''' into v_clean_flag;
                while v_clean_flag !=0 loop
                    /*select
                    'CALL PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                    into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                    where s.table_name = V_TBNAME;
                    execute immediate v_ssql;*/
                    execute immediate 'DELETE FROM '||V_TBNAME||' partition('||v_partition_name_clean||') where net_type = ''4G''';
                    commit;
                    execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name_clean||') where net_type = ''4G''' into v_clean_flag;
                end loop;
        end if;  
        
        V_TBNAME := 'TB_INDEX_CELL_DAY';
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
        --准备工作完成！！


        --SQL_2：全网
        SQL_2 := '
        select s_date, t1.area, t1.area_level, problem_type, ''4G'' net_type, /*good_cell_num,*/ t2.cell_sum-good_cell_num as bad_cell_num, t2.cell_sum from
        (

                select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-4G接通率'' problem_type, count(JTL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where JTL_BADFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-4G掉线率'' problem_type, count(DXL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where DXL_BADFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-4G高干扰'' problem_type, count(AVG_RTWP_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where AVG_RTWP_BADFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-CSFB成功率'' problem_type, count(CSFB_CGL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where CSFB_CGL_BADFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-4G同频切换'' problem_type, count(TPQH_CGL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where TPQH_CGL_BADFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''质差-4GMR弱覆盖'' problem_type, count(MR_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where MR_BADFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-4G接通率'' problem_type, count(JTL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where JTL_TBFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-4G掉线率小区'' problem_type, count(DXL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where DXL_TBFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-4G高干扰'' problem_type, count(AVG_RTWP_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where AVG_RTWP_TBFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-4G上下行总流量'' problem_type, count(LL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where LL_TBFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-CSFB成功率'' problem_type, count(CSFB_CGL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where CSFB_CGL_TBFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-4G同频切换'' problem_type, count(TPQH_CGL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where TPQH_CGL_TBFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-4GTA覆盖距离'' problem_type, count(TA_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where TA_TBFLAG = 0 group by s_date
                union all
                select s_date, ''上海'' as area, ''上海'' as area_level, ''突变-4GMR弱覆盖'' problem_type, count(MR_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where MR_TBFLAG = 0 group by s_date
        )t1,
        (
            select ''上海'' as area, ''上海'' as area_level, count(distinct ecgi) cell_sum from TB_INDEX_CELL_DAY partition('||v_partition_name||')--全网指标，不关联DT 
        )t2
        where t1.area_level = t2.area_level';
        --dbms_output.put_line(SQL_2);

        --SQL_1：全网
        SQL_1 := '
        select s_date, t1.area, t1.area_level, problem_type, ''4G'' net_type, /*good_cell_num,*/ t2.cell_sum-good_cell_num as bad_cell_num, t2.cell_sum from
        (
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''质差-4G接通率'' problem_type, count(JTL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where JTL_BADFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''质差-4G掉线率'' problem_type, count(DXL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where DXL_BADFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''质差-4G高干扰'' problem_type, count(AVG_RTWP_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where AVG_RTWP_BADFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''质差-CSFB成功率'' problem_type, count(CSFB_CGL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where CSFB_CGL_BADFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''质差-4G同频切换'' problem_type, count(TPQH_CGL_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where TPQH_CGL_BADFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''质差-4GMR弱覆盖'' problem_type, count(MR_BADFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where MR_BADFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''突变-4G接通率'' problem_type, count(JTL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where JTL_TBFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''突变-4G掉线率小区'' problem_type, count(DXL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where DXL_TBFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''突变-4G高干扰'' problem_type, count(AVG_RTWP_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where AVG_RTWP_TBFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''突变-4G上下行总流量'' problem_type, count(LL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where LL_TBFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''突变-CSFB成功率'' problem_type, count(CSFB_CGL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where CSFB_CGL_TBFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''突变-4G同频切换'' problem_type, count(TPQH_CGL_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where TPQH_CGL_TBFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''突变-4GTA覆盖距离'' problem_type, count(TA_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where TA_TBFLAG = 0 group by s_date, COUNTY
            union all
            select s_date, ''优化分区'' as area, COUNTY as area_level, ''突变-4GMR弱覆盖'' problem_type, count(MR_TBFLAG) good_cell_num from TB_INDEX_CELL_DAY partition('||v_partition_name||') t where MR_TBFLAG = 0 group by s_date, COUNTY
        )t1,
        (
            select tb.COUNTY as area_level_dt, count(tb.ecgi) cell_sum
            from
            (
                select distinct ecgi, COUNTY from TB_INDEX_CELL_DAY partition('||v_partition_name||')
            )tb,
            (
                select enb_id*256 + ci as ecgi, county from DT_CELL_L
            )dt
            where tb.ecgi = dt.ecgi and tb.county is not null
            group by tb.COUNTY
        )t2
        where t1.area_level = t2.area_level_dt';
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

        V_TBNAME := 'OSS_CELL_NUM';
        PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_LOCATE(V_TBNAME, V_DATE_THRESHOLD_START, V_PARTITION_NAME);
        
        /*open cur_partition; --开始索引字典表
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
        close cur_partition;--------------*/
        
        --入库数量判断
        execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name_clean||') where net_type = ''4G''' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name_clean||')
        where net_type = ''4G'' group by s_date, area, area_level, problem_type having count(1)>1)'into v_insert_repeat;
        dbms_output.put_line('表 '||v_tbname||' 天级数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');
        
    
    END PROC_TB_OSS_DAY;

-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --in：TB_LIST_CELL_HOUR_TBZC
    --out：TB_EXPORT_TMP
    PROCEDURE PROC_TB_LIST_CELL_HOUR_FORMAT(V_DATE_THRESHOLD_START VARCHAR2) IS --小区小时指标临时表
    --v_high_value_vc                 varchar2(20);
    --v_partition_name                varchar2(50);
    --v_partition_name_7_ago          varchar2(50);
    /*v_mr_partition_name                varchar2(50);
    v_mr_partition_name_7_ago          varchar2(50);*/
    V_TBNAME                        varchar2(100);
    V_DATE_THRESHOLD_END            varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd'); --20191004

    V_DATE_THRESHOLD_START_7_AGO    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 7, 'yyyymmdd'); --2019/10/03 - 7 = 2019/09/26 → 20190926
    V_DATE_THRESHOLD_END_7_AGO      varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 6, 'yyyymmdd'); --20190927
    --sql_2                           varchar2(4000);

    v_insert_cnt   number;
    --v_insert_repeat   number;
    --v_ssql varchar2(500);
    --v_clean_flag number;
    v_timeout   date;

    j number := 0;

    type TBZC_EXPORT_3_TYPE is record
    (
        s_date                  DATE,
        s_hour                  INTEGER,
        enb_id                  INTEGER,
        cell_id                 INTEGER,
        ecgi                    INTEGER,
        /*jtl_badflag             VARCHAR2(5),
        dxl_badflag             VARCHAR2(5),
        avg_rtwp_badflag        VARCHAR2(5),
        csfb_cgl_badflag        VARCHAR2(5),
        tpqh_cgl_badflag        VARCHAR2(5),
        mr_badflag              VARCHAR2(5),
        jtl_tbflag              VARCHAR2(5),
        dxl_tbflag              VARCHAR2(5),
        avg_rtwp_tbflag         VARCHAR2(5),
        ll_tbflag               VARCHAR2(5),
        csfb_cgl_tbflag         VARCHAR2(5),
        tpqh_cgl_tbflag         VARCHAR2(5),
        ta_tbflag               VARCHAR2(5),
        mr_tbflag               VARCHAR2(5),*/
        lte_jtl                 VARCHAR2(32),
        lte_jtl_wk_ago          VARCHAR2(32),
        lte_dxl                 VARCHAR2(32),
        lte_dxl_wk_ago          VARCHAR2(32),
        avg_rtwp                VARCHAR2(32),
        avg_rtwp_wk_ago         VARCHAR2(32),
        lte_ll                  VARCHAR2(32),
        lte_ll_wk_ago           VARCHAR2(32),
        ll_qam_zb               VARCHAR2(32),
        ll_qam_zb_wk_ago        VARCHAR2(32),
        ll_gzsl                 VARCHAR2(32),
        ll_gzsl_wk_ago          VARCHAR2(32),
        csfb_cgl                VARCHAR2(32),
        csfb_cgl_wk_ago         VARCHAR2(32),
        tpqh_cgl                VARCHAR2(32),
        tpqh_cgl_wk_ago         VARCHAR2(32),
        ta                      VARCHAR2(32),
        ta_wk_ago               VARCHAR2(32),
        rsrp_110_rate           VARCHAR2(32),
        rsrp_110_rate_wk_ago    VARCHAR2(32),
        rsrp_all_simples        VARCHAR2(32),
        rsrp_all_simples_wk_ago VARCHAR2(32),
        lte_dxl_n        VARCHAR2(32),
        lte_dxl_n_wk_ago        VARCHAR2(32)
    );
    --list_tmp_1 tbzc_export_3_type;

    -- 定义基于记录的嵌套表
    TYPE NESTED_EXPORT_TYPE IS TABLE OF TBZC_EXPORT_3_TYPE;
    -- 声明集合变量
    EXPORT_TAB          NESTED_EXPORT_TYPE;


    -- 定义了一个变量来作为limit的值
    V_LIMIT PLS_INTEGER := 1000;
    -- 定义变量来记录FETCH次数
    V_COUNTER INTEGER := 0;


    cursor CUR_SQL_2 is
    select S_DATE, S_HOUR, ENB_ID, CELL_ID, ECGI,
    /*JTL_BADFLAG, DXL_BADFLAG, AVG_RTWP_BADFLAG, CSFB_CGL_BADFLAG, TPQH_CGL_BADFLAG, MR_BADFLAG,
    JTL_TBFLAG, DXL_TBFLAG, AVG_RTWP_TBFLAG, LL_TBFLAG, CSFB_CGL_TBFLAG, TPQH_CGL_TBFLAG, TA_TBFLAG, MR_TBFLAG,*/
    LTE_JTL, LTE_JTL_WK_AGO, LTE_DXL, LTE_DXL_WK_AGO, AVG_RTWP, AVG_RTWP_WK_AGO,
    LTE_LL, LTE_LL_WK_AGO, LL_QAM_ZB, LL_QAM_ZB_WK_AGO, LL_GZSL, LL_GZSL_WK_AGO,
    CSFB_CGL, CSFB_CGL_WK_AGO, TPQH_CGL, TPQH_CGL_WK_AGO, TA, TA_WK_AGO, RSRP_110_RATE, RSRP_110_RATE_WK_AGO,
    RSRP_ALL_SIMPLES, RSRP_ALL_SIMPLES_WK_AGO,
    LTE_DXL_N, LTE_DXL_N_WK_AGO
    
    /*,
    PROVINCE, VENDOR_CELL_ID, COUNTY, VENDOR_ID, RESERVED3, RESERVED8, TOWN_ID,
    RNC, RESERVED4, RESERVED5, COVER_TYPE, PRE_FIELD4, PRE_FIELD5, LIFE, LON, LAT*/
    from
    (
        select
        t1.s_date, t1.s_hour, t1.enb_id, t1.cell_id, t1.ecgi,
        /*jtl_badflag, dxl_badflag, avg_rtwp_badflag, csfb_cgl_badflag, tpqh_cgl_badflag, mr_badflag,
        jtl_tbflag, dxl_tbflag, avg_rtwp_tbflag, ll_tbflag, csfb_cgl_tbflag, tpqh_cgl_tbflag, ta_tbflag, mr_tbflag,*/
        t1.lte_jtl, t2.lte_jtl as lte_jtl_wk_ago,
        t1.lte_dxl, t2.lte_dxl as lte_dxl_wk_ago,
        t1.avg_rtwp, t2.avg_rtwp as avg_rtwp_wk_ago,
        t1.lte_ll, t2.lte_ll as lte_ll_wk_ago,
        t1.ll_qam_zb, t2.ll_qam_zb as ll_qam_zb_wk_ago,
        t1.ll_gzsl, t2.ll_gzsl as ll_gzsl_wk_ago,
        t1.csfb_cgl, t2.csfb_cgl as csfb_cgl_wk_ago,
        t1.tpqh_cgl, t2.tpqh_cgl as tpqh_cgl_wk_ago,
        t1.ta, t2.ta as ta_wk_ago,
        t1.rsrp_110_rate, t2.rsrp_110_rate as rsrp_110_rate_wk_ago,
        t1.rsrp_all_simples, t2.rsrp_all_simples as rsrp_all_simples_wk_ago,
        t1.lte_dxl_n, t2.lte_dxl_n as lte_dxl_n_wk_ago --20191108添加，4G掉线次数
        
        /*,
        province, vendor_cell_id, county, vendor_id, reserved3, reserved8,
        town_id, rnc, reserved4, reserved5, cover_type, dt.pre_field4, dt.pre_field5, life, lon, lat*/
        from
        (
            select * from TB_LIST_CELL_HOUR_TBZC t
            --where t.s_date = to_date(20191003, 'yyyymmdd') --and t.ecgi = '8397606'
            where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
            --and s_hour = (case when V_DATE_HOUR is null then s_hour else cast(V_DATE_HOUR as int) end)
        )t1
        left join
        (
            select s_date, s_hour, enb_id, cell_id, ecgi, lte_jtl, lte_jtl_n, lte_jtl_d, lte_dxl, lte_dxl_n, lte_dxl_d, avg_rtwp, lte_ll, ll_qam_zb, ll_gzsl, csfb_cgl, csfb_cgl_n, csfb_cgl_d, tpqh_cgl, tpqh_cgl_n, tpqh_cgl_d, ta, rsrp_110_rate, rsrp_all_simples
            from TB_LIST_CELL_HOUR_TBZC t
            --where t.s_date = to_date(20190926, 'yyyymmdd')/*to_date(20191003, 'yyyymmdd') -7*/ --and t.ecgi = '8397606'
            where s_date >= to_date(V_DATE_THRESHOLD_START_7_AGO ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END_7_AGO ,'yyyymmdd')
            --and s_hour = (case when V_DATE_HOUR is null then s_hour else cast(V_DATE_HOUR as int) end)
        )t2
        on t1.ecgi = t2.ecgi and t1.s_hour = t2.s_hour
        /*left join
        (
            select t.enb_id*256 + t.ci as ecgi, t.pre_field4, t.pre_field5 from dt_cell_l t
        ) dt
        on t2.ecgi = dt.ecgi*/
    );


    --多行转多列
    --详见 PROC_TB_LIST_CELL_HOUR_FORMAT2
    
    BEGIN
        V_TBNAME := 'TB_EXPORT_TMP';
       --程序超时判定的起始时间设置
        v_timeout := sysdate;
        
        --只作为每次数据的中转，不保留！！！
        execute immediate 'truncate table TB_EXPORT_TMP';-- using V_TBNAME;

        ---------
        OPEN CUR_SQL_2; --DATA_CUR
        /*FETCH CUR_SQL_2 INTO EXPORT_FORMAT;
        LOOP
            exit when NOT (cur_sql_2%FOUND) or round(to_number(sysdate - v_timeout) * 24 * 60) >= 13;*/
        LOOP
            FETCH CUR_SQL_2 BULK COLLECT INTO EXPORT_TAB LIMIT V_LIMIT;
            EXIT WHEN EXPORT_TAB.count = 0 or round(to_number(sysdate - v_timeout) * 24 * 60) >= 13;
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条


            FOR I IN EXPORT_TAB.FIRST .. EXPORT_TAB.LAST
            LOOP
            j := j + 1;
            
            --执行插入
            execute immediate 'insert /*+append*/ into TB_EXPORT_TMP
            values(
            :s_date, :s_hour, :enb_id, :cell_id, :ecgi,
            :lte_jtl,
            :lte_jtl_wk_ago,
            :lte_dxl,
            :lte_dxl_wk_ago,
            :avg_rtwp,
            :avg_rtwp_wk_ago,
            :lte_ll,
            :lte_ll_wk_ago,
            :ll_qam_zb,
            :ll_qam_zb_wk_ago,
            :ll_gzsl,
            :ll_gzsl_wk_ago,
            :csfb_cgl,
            :csfb_cgl_wk_ago,
            :tpqh_cgl,
            :tpqh_cgl_wk_ago,
            :ta,
            :ta_wk_ago,
            :rsrp_110_rate,
            :rsrp_110_rate_wk_ago,
            :rsrp_all_simples,
            :rsrp_all_simples_wk_ago,
            :lte_dxl_n,
            :lte_dxl_n_wk_ago
            )'
            using
            EXPORT_TAB(I).s_date, EXPORT_TAB(I).s_hour, EXPORT_TAB(I).enb_id, EXPORT_TAB(I).cell_id, EXPORT_TAB(I).ecgi,
            EXPORT_TAB(I).lte_jtl,
            EXPORT_TAB(I).lte_jtl_wk_ago,
            EXPORT_TAB(I).lte_dxl,
            EXPORT_TAB(I).lte_dxl_wk_ago,
            EXPORT_TAB(I).avg_rtwp,
            EXPORT_TAB(I).avg_rtwp_wk_ago,
            EXPORT_TAB(I).lte_ll,
            EXPORT_TAB(I).lte_ll_wk_ago,
            EXPORT_TAB(I).ll_qam_zb,
            EXPORT_TAB(I).ll_qam_zb_wk_ago,
            EXPORT_TAB(I).ll_gzsl,
            EXPORT_TAB(I).ll_gzsl_wk_ago,
            EXPORT_TAB(I).csfb_cgl,
            EXPORT_TAB(I).csfb_cgl_wk_ago,
            EXPORT_TAB(I).tpqh_cgl,
            EXPORT_TAB(I).tpqh_cgl_wk_ago,
            EXPORT_TAB(I).ta,
            EXPORT_TAB(I).ta_wk_ago,
            EXPORT_TAB(I).rsrp_110_rate,
            EXPORT_TAB(I).rsrp_110_rate_wk_ago,
            EXPORT_TAB(I).rsrp_all_simples,
            EXPORT_TAB(I).rsrp_all_simples_wk_ago,
            EXPORT_TAB(I).lte_dxl_n,
            EXPORT_TAB(I).lte_dxl_n_wk_ago;


            if mod(j, 500)=0 --commit every 100 times
                then commit;
            end if;
            
            END LOOP;
        END LOOP;
        COMMIT;

        close CUR_SQL_2;
        dbms_output.put_line('j: '||j);

        --入库数量判断
        execute immediate 'select count(1) from '||v_tbname into v_insert_cnt;
        -- --重复率判断
        -- execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        -- group by s_date, ecgi, style having count(1)>1)'into v_insert_repeat;
        dbms_output.put_line('临时表(导出汇聚用) '||v_tbname||' 小时级数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||' 行.
        ');
    END PROC_TB_LIST_CELL_HOUR_FORMAT;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    PROCEDURE PROC_TB_LIST_CELL_HOUR_FORMAT2(V_DATE_THRESHOLD_START VARCHAR2, V_DATE_HOUR VARCHAR2 := 23) AS --小区小时指标
    --in：TB_EXPORT_TMP / DT_CELL_L
    --out：TB_EXPORT
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
    
    
    
    type format_type is record
    (
        s_date          date,
        --s_hour          integer,
        enb_id          integer,
        cell_id         integer,
        ecgi            integer,
        style           varchar2(50),
        --data_ago_cur  varchar2(4000),
        kpi_9            VARCHAR2(100),
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
        vendor_cell_id  VARCHAR2(32),
        county          VARCHAR2(32),
        vendor_id       VARCHAR2(32),
        reserved3       VARCHAR2(32),
        reserved8       VARCHAR2(32),
        town_id         VARCHAR2(32),
        rnc             VARCHAR2(32),
        reserved4       VARCHAR2(32),
        reserved5       VARCHAR2(32),
        cover_type      VARCHAR2(32),
        pre_field4      VARCHAR2(32),
        pre_field5      VARCHAR2(100),
        life            VARCHAR2(32),
        lon             VARCHAR2(32),
        lat             VARCHAR2(32)
    );
    --export_format format_type;

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
    select ex1.s_date, ex1.enb_id, ex1.cell_id, ex1.ecgi, ex1.style,
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
    PROVINCE, VENDOR_CELL_ID, COUNTY, VENDOR_ID, RESERVED3, RESERVED8, TOWN_ID,
    RNC, RESERVED4, RESERVED5, COVER_TYPE, PRE_FIELD4, PRE_FIELD5, LIFE, LON, LAT
    from
    (
        select s_date, /*s_hour, */enb_id, cell_id, ecgi, style, listagg(data_ago_cur,',') within group (order by s_hour) as data_ago_cur
        from
        (
            select s_date, s_hour, enb_id, cell_id, ecgi, style, /*zcflag||'/'||tbflag flag_zc_tb,*/ s_hour||':'||data_wk_ago||'\'||data_cur as data_ago_cur from
            (
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G接通率' as style, /* jtl_badflag as zcflag, jtl_tbflag as tbflag, */ lte_jtl as data_cur, lte_jtl_wk_ago as data_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G掉线率', to_char(lte_dxl, 'fm990.00')||'~'||lte_dxl_n as data_cur, to_char(lte_dxl_wk_ago, 'fm990.00')||'~'||lte_dxl_n_wk_ago as data_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G高干扰', /* avg_rtwp_badflag, avg_rtwp_tbflag, */ avg_rtwp, avg_rtwp_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G流量', /* '', ll_tbflag, */ to_char(lte_ll, 'fm990.00'), to_char(lte_ll_wk_ago, 'fm990.00') from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, 'CSFB', /* csfb_cgl_badflag, csfb_cgl_tbflag, */ csfb_cgl, csfb_cgl_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G切换', /* tpqh_cgl_badflag, tpqh_cgl_tbflag, */ tpqh_cgl, tpqh_cgl_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4GTA', /* '', ta_tbflag, */ ta, ta_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4GMR', /* mr_badflag, mr_tbflag, */ rsrp_110_rate, rsrp_110_rate_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
            )-- order by ecgi, s_hour
            where s_hour <= (case when (V_DATE_HOUR is null or V_DATE_HOUR >=16) then 16 else cast(V_DATE_HOUR as int) end) --加限制，每小时出数据
            --and ecgi = 8396812
        )  group by s_date, enb_id, cell_id, ecgi, style--, data_ago_cur
    )ex1
    left join
    (
        select s_date, ecgi, style, listagg(data_ago_cur,',') within group (order by s_hour) as data_ago_cur
        from
        (
            select s_date, s_hour, enb_id, cell_id, ecgi, style, /*zcflag||'/'||tbflag flag_zc_tb,*/ s_hour||':'||data_wk_ago||'\'||data_cur as data_ago_cur from
            (
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G接通率' as style, /* jtl_badflag as zcflag, jtl_tbflag as tbflag, */ lte_jtl as data_cur, lte_jtl_wk_ago as data_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G掉线率', to_char(lte_dxl, 'fm990.00')||'~'||lte_dxl_n as data_cur, to_char(lte_dxl_wk_ago, 'fm990.00')||'~'||lte_dxl_n_wk_ago as data_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G高干扰', /* avg_rtwp_badflag, avg_rtwp_tbflag, */ avg_rtwp, avg_rtwp_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G流量', /* '', ll_tbflag, */ to_char(lte_ll, 'fm990.00'), to_char(lte_ll_wk_ago, 'fm990.00') from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, 'CSFB', /* csfb_cgl_badflag, csfb_cgl_tbflag, */ csfb_cgl, csfb_cgl_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G切换', /* tpqh_cgl_badflag, tpqh_cgl_tbflag, */ tpqh_cgl, tpqh_cgl_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4GTA', /* '', ta_tbflag, */ ta, ta_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4GMR', /* mr_badflag, mr_tbflag, */ rsrp_110_rate, rsrp_110_rate_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                and s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
            )-- order by ecgi, s_hour
            --where s_hour between V_DATE_HOUR_START_1723 and V_DATE_HOUR_END_1723 --加限制，每小时出数据，负责14~18
            --and ecgi = 8396812
        )  group by s_date, enb_id, cell_id, ecgi, style--, data_ago_cur
    )ex2
    on ex1.ecgi = ex2.ecgi and ex1.s_date = ex2.s_date and ex1.style =ex2.style
    /*left join
    (
        select s_date, \*s_hour, enb_id, cell_id,*\ ecgi, style, listagg(data_ago_cur,',') within group (order by s_hour) as data_ago_cur
        from
        (
            select s_date, s_hour, enb_id, cell_id, ecgi, style, \*zcflag||'/'||tbflag flag_zc_tb,*\ s_hour||':'||data_wk_ago||'/'||data_cur as data_ago_cur from
            (
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G接通率' as style, \* jtl_badflag as zcflag, jtl_tbflag as tbflag, *\ lte_jtl as data_cur, lte_jtl_wk_ago as data_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                where s_hour between cast(V_DATE_HOUR_START_1923 as int) and cast(V_DATE_HOUR_END_1923 as int) --加限制，每小时出数据，负责19~23
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G掉线率', \* dxl_badflag, dxl_tbflag, *\ to_char(lte_dxl, 'fm990.00'), to_char(lte_dxl_wk_ago, 'fm990.00') from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                where s_hour between cast(V_DATE_HOUR_START_1923 as int) and cast(V_DATE_HOUR_END_1923 as int) --加限制，每小时出数据，负责19~23
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G高干扰', \* avg_rtwp_badflag, avg_rtwp_tbflag, *\ avg_rtwp, avg_rtwp_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                where s_hour between cast(V_DATE_HOUR_START_1923 as int) and cast(V_DATE_HOUR_END_1923 as int) --加限制，每小时出数据，负责19~23
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G流量', \* '', ll_tbflag, *\ to_char(lte_ll, 'fm990.00'), to_char(lte_ll_wk_ago, 'fm990.00') from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                where s_hour between cast(V_DATE_HOUR_START_1923 as int) and cast(V_DATE_HOUR_END_1923 as int) --加限制，每小时出数据，负责19~23
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, 'CSFB', \* csfb_cgl_badflag, csfb_cgl_tbflag, *\ csfb_cgl, csfb_cgl_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                where s_hour between cast(V_DATE_HOUR_START_1923 as int) and cast(V_DATE_HOUR_END_1923 as int) --加限制，每小时出数据，负责19~23
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G切换', \* tpqh_cgl_badflag, tpqh_cgl_tbflag, *\ tpqh_cgl, tpqh_cgl_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                where s_hour between cast(V_DATE_HOUR_START_1923 as int) and cast(V_DATE_HOUR_END_1923 as int) --加限制，每小时出数据，负责19~23
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4GTA', \* '', ta_tbflag, *\ ta, ta_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                where s_hour between cast(V_DATE_HOUR_START_1923 as int) and cast(V_DATE_HOUR_END_1923 as int) --加限制，每小时出数据，负责19~23
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4GMR', \* mr_badflag, mr_tbflag, *\ rsrp_110_rate, rsrp_110_rate_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                where s_hour between cast(V_DATE_HOUR_START_1923 as int) and cast(V_DATE_HOUR_END_1923 as int) --加限制，每小时出数据，负责19~23
            )-- order by ecgi, s_hour
            --where s_hour between cast(V_DATE_HOUR_START_1923 as int) and cast(V_DATE_HOUR_END_1923 as int) --加限制，每小时出数据，负责19~23
            --and ecgi = 8396812
        )  group by s_date, enb_id, cell_id, ecgi, style
    )ex3
    on ex1.ecgi = ex3.ecgi and ex1.s_date = ex3.s_date and ex1.style =ex3.style*/
    left join
    (
        select t.enb_id*256 + t.ci as ecgi, t.* from dt_cell_l t
    ) dt
    on ex1.ecgi = dt.ecgi
    ;



    BEGIN
       
       --程序超时判定的起始时间设置
        v_timeout := sysdate;

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
                    /*V_DATE_HOUR_START_1923 := 404;
                    V_DATE_HOUR_END_1923 := 404;*/
        /*elsif V_DATE_HOUR >= 19 and V_DATE_HOUR <= 23 --19~23
            then    V_DATE_HOUR_START_1723 := 14;
                    V_DATE_HOUR_END_1723 := 18;
                    V_DATE_HOUR_START_1923 := 19;
                    V_DATE_HOUR_END_1923 := V_DATE_HOUR;*/
        end if;
        
        
        
        V_TBNAME := 'TB_EXPORT';
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

        
       /* if V_DATE_HOUR is not null
          then
              --清理
              execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||') where s_hour='||i_date_hour into v_clean_flag;
              while v_clean_flag !=0 loop
                  execute immediate 'delete from '||v_tbname||' partition('||v_partition_name||') where s_hour='||i_date_hour;
                  commit;
                  \*select
                  'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                  into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                  where s.table_name = v_tbname;
                  execute immediate v_ssql;*\
                  --select count(1) into v_clean_flag from ZC_CELL_LIST_2G where s_date >= v_date_start and s_date < v_date_end;
                  execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||') where s_hour='||i_date_hour into v_clean_flag;
              end loop;
          elsif V_DATE_HOUR is null
          then
              --清理
              execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
              while v_clean_flag !=0 loop
                  select
                  'CALL PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                  into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                  where s.table_name = V_TBNAME;
                  execute immediate v_ssql;
                  execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
              end loop;
        end if;*/
        
        --清理
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


        ---------
        OPEN CUR_SQL_2; --DATA_CUR
        /*FETCH CUR_SQL_2 INTO EXPORT_FORMAT;
        LOOP
            exit when NOT (cur_sql_2%FOUND) or round(to_number(sysdate - v_timeout) * 24 * 60) >= 13;*/
        LOOP
            FETCH CUR_SQL_2 BULK COLLECT INTO EXPORT_TAB LIMIT V_LIMIT;
            EXIT WHEN EXPORT_TAB.count = 0 or round(to_number(sysdate - v_timeout) * 24 * 60) >= 13;
            V_COUNTER := V_COUNTER + 1; -- 记录使用LIMIT之后fetch的次数，一次500条


            FOR I IN EXPORT_TAB.FIRST .. EXPORT_TAB.LAST
            LOOP


            j := j + 1;

            --执行插入
            execute immediate 'insert /*+append*/ into TB_EXPORT
            values(
            :s_date,
            :enb_id,
            :cell_id,
            :ecgi,
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
            :province,
            :vendor_cell_id,
            :county,
            :vendor_id,
            :reserved3,
            :reserved8,
            :town_id,
            :rnc,
            :reserved4,
            :reserved5,
            :cover_type,
            :pre_field4 , :pre_field5,
            :life,
            :lon,
            :lat)'
            using
            EXPORT_TAB(I).s_date,
            --EXPORT_TAB(I).s_hour,
            EXPORT_TAB(I).enb_id,
            EXPORT_TAB(I).cell_id,
            EXPORT_TAB(I).ecgi,
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
/*            EXPORT_TAB(I).kpi_9_wk,
            EXPORT_TAB(I).kpi_9_cur,
            EXPORT_TAB(I).kpi_10_wk,
            EXPORT_TAB(I).kpi_10_cur,
            EXPORT_TAB(I).kpi_11_wk,
            EXPORT_TAB(I).kpi_11_cur,
            EXPORT_TAB(I).kpi_12_wk,
            EXPORT_TAB(I).kpi_12_cur,
            EXPORT_TAB(I).kpi_13_wk,
            EXPORT_TAB(I).kpi_13_cur,
            EXPORT_TAB(I).kpi_14_wk,
            EXPORT_TAB(I).kpi_14_cur,
            EXPORT_TAB(I).kpi_15_wk,
            EXPORT_TAB(I).kpi_15_cur,
            EXPORT_TAB(I).kpi_16_wk,
            EXPORT_TAB(I).kpi_16_cur,
            EXPORT_TAB(I).kpi_17_wk,
            EXPORT_TAB(I).kpi_17_cur,
            EXPORT_TAB(I).kpi_18_wk,
            EXPORT_TAB(I).kpi_18_cur,
            EXPORT_TAB(I).kpi_19_wk,
            EXPORT_TAB(I).kpi_19_cur,
            EXPORT_TAB(I).kpi_20_wk,
            EXPORT_TAB(I).kpi_20_cur,
            EXPORT_TAB(I).kpi_21_wk,
            EXPORT_TAB(I).kpi_21_cur,
            EXPORT_TAB(I).kpi_22_wk,
            EXPORT_TAB(I).kpi_22_cur,
            EXPORT_TAB(I).kpi_23_wk,
            EXPORT_TAB(I).kpi_23_cur,*/
            EXPORT_TAB(I).province,
            EXPORT_TAB(I).vendor_cell_id,
            EXPORT_TAB(I).county,
            EXPORT_TAB(I).vendor_id,
            EXPORT_TAB(I).reserved3,
            EXPORT_TAB(I).reserved8,
            EXPORT_TAB(I).town_id,
            EXPORT_TAB(I).rnc,
            EXPORT_TAB(I).reserved4,
            EXPORT_TAB(I).reserved5,
            EXPORT_TAB(I).cover_type,
            EXPORT_TAB(I).pre_field4,
            EXPORT_TAB(I).pre_field5,
            EXPORT_TAB(I).life,
            EXPORT_TAB(I).lon,
            EXPORT_TAB(I).lat
            ;
            if mod(j, 100)=0 --commit every 100 times
                then commit;
                if mod(j, 100000) = 0
                then dbms_output.put_line('j: '||j);
                end if;
            end if;
            --fetch cur_sql_2 into export_format;
            
            --超时判定
            if round(to_number(sysdate - v_timeout) * 24 * 60) >= 13 then return;
            end if;
            
            END LOOP;
            
        END LOOP;
        commit;

        --if sysdate - v_timeout 
        close cur_sql_2;

        dbms_output.put_line('j: '||j);
        
        --入库数量判断
        execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||')' into v_insert_cnt;
        --重复率判断
        /*execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        group by s_date, ecgi, style having count(1)>1)'into v_insert_repeat;*/
        dbms_output.put_line('表 '||v_tbname||' 天级数据迭代更新完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||' 行.
        ');
        
        
    END PROC_TB_LIST_CELL_HOUR_FORMAT2;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
/*PROCEDURE PROC_TB_LIST_CELL_HOUR_FORMAT4(V_DATE_THRESHOLD_START VARCHAR2, V_DATE_HOUR VARCHAR2 := 23) AS --小区小时指标
    --in：TB_EXPORT_TMP / DT_CELL_L
    --out：TB_EXPORT
    v_high_value_vc                 varchar2(20);
    v_partition_name                varchar2(50);
    --v_partition_name_7_ago          varchar2(50);
    \*v_mr_partition_name                varchar2(50);
    v_mr_partition_name_7_ago          varchar2(50);*\
    V_TBNAME                        varchar2(100);
    V_DATE_THRESHOLD_END            varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') + 1, 'yyyymmdd'); --20191004

    --V_DATE_THRESHOLD_START_7_AGO    varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 7, 'yyyymmdd'); --2019/10/03 - 7 = 2019/09/26 → 20190926
    --V_DATE_THRESHOLD_END_7_AGO      varchar2(8) := to_char(to_date(V_DATE_THRESHOLD_START, 'yyyymmdd') - 6, 'yyyymmdd'); --20190927
    --sql_2                           varchar2(4000);

    v_insert_cnt   number;
    v_insert_repeat   number;
    --v_ssql varchar2(500);
    --v_clean_flag number;

    i number := 0;
    --v_timeout   date;
    --i_date_hour varchar2(20);
    
    type format_type is record
    (
        s_date          date,
        --s_hour          integer,
        enb_id          integer,
        cell_id         integer,
        ecgi            integer,
        style           varchar2(50),
        --data_ago_cur  varchar2(4000),
        kpi_9            VARCHAR2(100),
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
        vendor_cell_id  VARCHAR2(32),
        county          VARCHAR2(32),
        vendor_id       VARCHAR2(32),
        reserved3       VARCHAR2(32),
        reserved8       VARCHAR2(32),
        town_id         VARCHAR2(32),
        rnc             VARCHAR2(32),
        reserved4       VARCHAR2(32),
        reserved5       VARCHAR2(32),
        cover_type      VARCHAR2(32),
        pre_field4      VARCHAR2(32),
        pre_field5      VARCHAR2(100),
        life            VARCHAR2(32),
        lon             VARCHAR2(32),
        lat             VARCHAR2(32)
    );
    export_format format_type;


    type partition_type is record(
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

    \*type curtyp is ref cursor;
    cur_sql_2 curtyp;*\

    --多行转多列
    cursor cur_sql_2 is
    select s_date, ex.enb_id, ex.cell_id, ex.ecgi, style,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,1,'i') , '[^:]+',1,2,'i' ) as KPI_9,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,2,'i') , '[^:]+',1,2,'i' ) as KPI_10,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,3,'i') , '[^:]+',1,2,'i' ) as KPI_11,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,4,'i') , '[^:]+',1,2,'i' ) as KPI_12,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,5,'i') , '[^:]+',1,2,'i' ) as KPI_13,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,6,'i') , '[^:]+',1,2,'i' ) as KPI_14,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,7,'i') , '[^:]+',1,2,'i' ) as KPI_15,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,8,'i') , '[^:]+',1,2,'i' ) as KPI_16,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,9,'i') , '[^:]+',1,2,'i' ) as KPI_17,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,10,'i') , '[^:]+',1,2,'i' ) as KPI_18,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,11,'i'), '[^:]+',1,2,'i' ) as KPI_19,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,12,'i'), '[^:]+',1,2,'i' ) as KPI_20,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,13,'i'), '[^:]+',1,2,'i' ) as KPI_21,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,14,'i'), '[^:]+',1,2,'i' ) as KPI_22,
    regexp_substr(regexp_substr(data_ago_cur, '[^,]+',1,15,'i'), '[^:]+',1,2,'i' ) as KPI_23,
    PROVINCE, VENDOR_CELL_ID, COUNTY, VENDOR_ID, RESERVED3, RESERVED8, TOWN_ID,
    RNC, RESERVED4, RESERVED5, COVER_TYPE, PRE_FIELD4, PRE_FIELD5, LIFE, LON, LAT
    from
    (
        select s_date, \*s_hour, *\enb_id, cell_id, ecgi, style, listagg(data_ago_cur,',') within group (order by s_hour) as data_ago_cur
        from
        (
            select s_date, s_hour, enb_id, cell_id, ecgi, style, \*zcflag||'/'||tbflag flag_zc_tb,*\ s_hour||':'||data_wk_ago||'/'||data_cur as data_ago_cur from
            (
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G接通率' as style, \* jtl_badflag as zcflag, jtl_tbflag as tbflag, *\ lte_jtl as data_cur, lte_jtl_wk_ago as data_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G掉线率', \* dxl_badflag, dxl_tbflag, *\ to_char(lte_dxl, 'fm990.00'), to_char(lte_dxl_wk_ago, 'fm990.00') from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G高干扰', \* avg_rtwp_badflag, avg_rtwp_tbflag, *\ avg_rtwp, avg_rtwp_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G流量', \* '', ll_tbflag, *\ to_char(lte_ll, 'fm990.00'), to_char(lte_ll_wk_ago, 'fm990.00') from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, 'CSFB', \* csfb_cgl_badflag, csfb_cgl_tbflag, *\ csfb_cgl, csfb_cgl_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4G切换', \* tpqh_cgl_badflag, tpqh_cgl_tbflag, *\ tpqh_cgl, tpqh_cgl_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4GTA', \* '', ta_tbflag, *\ ta, ta_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
                union all
                select s_date, s_hour, enb_id, cell_id, ecgi, '4GMR', \* mr_badflag, mr_tbflag, *\ rsrp_110_rate, rsrp_110_rate_wk_ago from TB_EXPORT_TMP where s_date >= to_date(V_DATE_THRESHOLD_START ,'yyyymmdd') and s_date < to_date(V_DATE_THRESHOLD_END ,'yyyymmdd')
            )-- order by ecgi, s_hour
            where s_hour <= V_DATE_HOUR --加限制，每小时出数据
            --and ecgi = 8396812
        )  group by s_date, enb_id, cell_id, ecgi, style--, data_ago_cur
    )ex
    left join
    (
        select t.enb_id*256 + t.ci as ecgi, t.* from dt_cell_l t
    ) dt
    on ex.ecgi = dt.ecgi
    ;

    sql_2  varchar2(2000) := 'merge into TB_EXPORT A
        using
        (
            select :1 as S_DATE, :2 as ENB_ID, :3 as CELL_ID, :4 as ECGI, :5 as DATA_STYLE,
            :6 as KPI_9 from dual
        )B
        on (A.s_date = B.s_date and A.ecgi = B.ecgi and A.style = B.DATA_STYLE)
        when matched then 
            update set 
            A.KPI_9  = B.KPI_9
        when not matched then 
            insert (A.S_DATE, A.ENB_ID, A.CELL_ID, A.ECGI, A.STYLE, 
            A.KPI_9)
            values(B.S_DATE, B.ENB_ID, B.CELL_ID, B.ECGI, B.DATA_STYLE, 
            B.KPI_9)';

    BEGIN
       
       --程序超时判定的起始时间设置
        --v_timeout := sysdate;


        V_TBNAME := 'TB_EXPORT';
        i := 0;
        --索引分区名
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

        \*if V_DATE_HOUR is null
          then i_date_hour := 'S_HOUR';
        else i_date_hour := V_DATE_HOUR;
        end if;*\
        
       \* if V_DATE_HOUR is not null
          then
              --清理
              execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||') where s_hour='||i_date_hour into v_clean_flag;
              while v_clean_flag !=0 loop
                  execute immediate 'delete from '||v_tbname||' partition('||v_partition_name||') where s_hour='||i_date_hour;
                  commit;
                  \*select
                  'call PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                  into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                  where s.table_name = v_tbname;
                  execute immediate v_ssql;*\
                  --select count(1) into v_clean_flag from ZC_CELL_LIST_2G where s_date >= v_date_start and s_date < v_date_end;
                  execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||') where s_hour='||i_date_hour into v_clean_flag;
              end loop;
          elsif V_DATE_HOUR is null
          then
              --清理
              execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
              while v_clean_flag !=0 loop
                  select
                  'CALL PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
                  into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
                  where s.table_name = V_TBNAME;
                  execute immediate v_ssql;
                  execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
              end loop;
        end if;*\
        
        --清理
        \*execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
        while v_clean_flag !=0 loop
            select
            'CALL PKG_MANAGE_SYSTEM_SHIN_PLUS8.PROC_PARTITION_TRUNCATE_RANGE('''|| table_name||''','''|| tm_grn ||''','''||V_DATE_THRESHOLD_START||''','''||V_DATE_THRESHOLD_START||''')'
            into v_ssql from FAST_DATA_PROCESS_TEMPLATE s
            where s.table_name = V_TBNAME;
            --execute immediate v_ssql;
            execute immediate 'select count(1) from '||V_TBNAME||' partition('||v_partition_name||')' into v_clean_flag;
        end loop;
        --准备工作完成！！*\

        
        ---------
        open cur_sql_2; --data_cur
        fetch cur_sql_2 into export_format;
        loop
            exit when NOT (cur_sql_2%FOUND) \*or round(to_number(sysdate - v_timeout) * 24 * 60) >= 13*\;

            i := i + 1;  
            execute immediate
            sql_2
            using
            export_format.s_date,
            export_format.enb_id,
            export_format.cell_id,
            export_format.ecgi,
            export_format.style,
            export_format.kpi_9\*,
            export_format.kpi_10,
            export_format.kpi_11,
            export_format.kpi_12,
            export_format.kpi_13,
            export_format.kpi_14,
            export_format.kpi_15,
            export_format.kpi_16,
            export_format.kpi_17,
            export_format.kpi_18,
            export_format.kpi_19,
            export_format.kpi_20,
            export_format.kpi_21,
            export_format.kpi_22,
            export_format.kpi_23,
            export_format.province,
            export_format.vendor_cell_id,
            export_format.county,
            export_format.vendor_id,
            export_format.reserved3,
            export_format.reserved8,
            export_format.town_id,
            export_format.rnc,
            export_format.reserved4,
            export_format.reserved5,
            export_format.cover_type,
            export_format.pre_field4,
            export_format.pre_field5,
            export_format.life,
            export_format.lon,
            export_format.lat*\;
            
            if mod(i, 100)=0 --commit every 100 times
                then commit;
                if mod(i, 100000) = 0
                then dbms_output.put_line('i: '||i);
                end if;
            end if;
            fetch cur_sql_2 into export_format;
            
            \*--超时判定
            if round(to_number(sysdate - v_timeout) * 24 * 60) >= 13 then return;
            end if;*\
        end loop;
        commit;

        --if sysdate - v_timeout 
        close cur_sql_2;
        dbms_output.put_line('i: '||i);
        
        --入库数量判断
        \*execute immediate 'select count(1) from '||v_tbname||' partition('||v_partition_name||')' into v_insert_cnt;
        --重复率判断
        execute immediate 'select count(1) from (select count(1) from '||v_tbname||' partition('||v_partition_name||')
        group by s_date, ecgi, style having count(1)>1)'into v_insert_repeat;*\
        dbms_output.put_line('表 '||v_tbname||' 小时级数据插入完成！时间戳：'||V_DATE_THRESHOLD_START||'，入库数据行数：'||v_insert_cnt||'，重复数据行数：'||v_insert_repeat||'行.
        ');

    END PROC_TB_LIST_CELL_HOUR_FORMAT4;*/
    
    
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --TB指标结果表周期性汇聚任务激活
    PROCEDURE ACTIVE_LC_ZCTB_AUTO AS
    V_DATE_START  varchar2(15);--天表任务激活时间戳
    V_DATE_HOUR    varchar2(15);--天表小时粒度激活时间戳
    
    V_DATE_START_8  varchar2(15);--天表任务激活时间戳

    v_loop_log number := 0;
    /*v_inside_loop_log number := 0;
    v_tbname varchar2(50);
    v_partition_name varchar2(30);
    v_exsit_flag number := 0;
    v_pkg_name varchar2(200);*/
    
    BEGIN
        --起止时间戳格式化，时间自动化，读取时间：sysdate--2019/10/22
        V_DATE_START :=  to_char(sysdate - numtodsinterval(3,'hour'),'yyyymmdd');--20191022
        V_DATE_HOUR  := /*23;*/ to_number(to_char(sysdate - numtodsinterval(3,'hour'), 'hh24'));--now：10:21 --->  8

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
            PKG_LC_INDEX_TB_CELL.PROC_TB_LIST_CELL_HOUR(V_DATE_START, V_DATE_HOUR);
            v_loop_log := v_loop_log +1;

            PKG_LC_INDEX_TB_CELL.PROC_TB_LIST_CELL_HOUR_TBZC(V_DATE_START, V_DATE_HOUR);
            v_loop_log := v_loop_log +1;

            PKG_LC_INDEX_TB_CELL.PROC_TB_INDEX_CELL_DAY(V_DATE_START);
            v_loop_log := v_loop_log +1;

            PKG_LC_INDEX_TB_CELL.PROC_TB_LIST_CELL_HOUR_FORMAT(V_DATE_START);
            v_loop_log := v_loop_log +1;

            PKG_LC_INDEX_TB_CELL.PROC_TB_LIST_CELL_HOUR_FORMAT2(V_DATE_START);
            v_loop_log := v_loop_log +1;

            dbms_output.put_line('TB小区小时级清单&天级标签周期性汇聚任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
            dbms_output.put_line('****------------------------------------------------------------------****
            ');
        elsif V_DATE_HOUR = 2
            then 
            PKG_LC_INDEX_TB_CELL.PROC_TB_OSS_DAY(V_DATE_START_8);
            v_loop_log := v_loop_log +1;
            dbms_output.put_line('TB小区天级区域数量统计任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
        else 
                dbms_output.put_line('TB周期性汇聚任务触发时间未至！！！：');
                return;
        end if;
        

    END ACTIVE_LC_ZCTB_AUTO;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    --TB数据补偿
    PROCEDURE ACTIVE_LC_ZCTB_SUPPLEMENT(V_DATE_THRESHOLD_START VARCHAR2) AS
    --V_DATE_START  varchar2(15);
    --V_DATE_MONDAY date;
    --V_DATE_MONTH   date;
    v_loop_log number := 0;
    BEGIN
        --起止时间戳格式化，时间自动化，读取时间：sysdate--2019/07/01
        --V_DATE_START :=  to_char(sysdate-1,'yyyymmdd');--20190630
        --每日执行昨日天级汇聚
        PKG_LC_INDEX_TB_CELL.PROC_TB_LIST_CELL_HOUR(V_DATE_THRESHOLD_START, '');
        v_loop_log := v_loop_log +1;

        PKG_LC_INDEX_TB_CELL.PROC_TB_LIST_CELL_HOUR_TBZC(V_DATE_THRESHOLD_START, '');
        v_loop_log := v_loop_log +1;

        PKG_LC_INDEX_TB_CELL.PROC_TB_INDEX_CELL_DAY(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;
            
        PKG_LC_INDEX_TB_CELL.PROC_TB_OSS_DAY(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;

        PKG_LC_INDEX_TB_CELL.PROC_TB_LIST_CELL_HOUR_FORMAT(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;

        PKG_LC_INDEX_TB_CELL.PROC_TB_LIST_CELL_HOUR_FORMAT2(V_DATE_THRESHOLD_START);
        v_loop_log := v_loop_log +1;



        dbms_output.put_line('TB小区小时级清单&天级标签标补偿任务完成！完成时间戳：'||to_char(sysdate,'yyyy/mm/dd hh24:mi:ss')||'，存储过程执行数量：'||v_loop_log||'.');
        dbms_output.put_line('****------------------------------------------------------------------****
        ');
    END ACTIVE_LC_ZCTB_SUPPLEMENT;


-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
    PROCEDURE PROC_TEST IS
    v_date_start  VARCHAR2(20);
    --v_ssql CLOB;
    /*i_tablename   varchar2(30);
    --v_loop_log number := 0 ;
    i_tablespace   varchar2(30);
    i_part_name   varchar2(30);
    i_part_date varchar2(30);
    i_partition_name varchar2(500);*/

    --v_clean_flag number;
    --v_proc_end_flag number :=0;
    BEGIN
        select 1 into v_date_start from dual;
        /*select dbms_output.put_line('234G质差小区清单天级汇聚任务完成！完成时间戳：'||sysdate) into v_ssql from dual;*/
        --select count(1) into v_date_start from sys.gv_$locked_object a;
    END PROC_TEST;


END PKG_LC_INDEX_TB_CELL;

/

