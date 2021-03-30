
CREATE OR REPLACE FUNCTION actv_comp (start_date date, end_date date)
RETURNS setof dw.dim_comp AS $$
BEGIN
    -- 考核开始日期<所选结束日期 且 考核结束日期>所选开始日期
    RETURN query SELECT * FROM dw.dim_comp
    WHERE open_dt<end_date AND COALESCE(close_dt, '9999-12-31')>start_date;
END;
$$ LANGUAGE plpgsql;

-- 取上次报表结束时间 cvrt_typ: last_rpt_end_dtm
-- 取今天报表结束时间 cvrt_typ: tdy_rpt_end_dtm
-- 取下一个工作日    cvrt_typ:  nxt_workday
-- 取季度的第一天    cvrt_typ： quar_start
-- 取下个季度的第一天 cvrt_typ： quar_end
-- 取当日业绩表数据时效性时间(如果当天批量结束，取批量时间，反之取当前时间) cvrt_typ：data_age_dtm
CREATE OR REPLACE FUNCTION cvrt_dt (sel_dt character varying(60), cvrt_typ character varying(60))
RETURNS varchar AS $cvrt_dt$
declare
    cvrt_dt varchar;
BEGIN
    -- 取上次报表结束时间(如果跨月，则取本月1日00:00:00)
    IF cvrt_typ='last_rpt_end_dtm'
    THEN
        SELECT CASE WHEN substring(report_date,6, 2)=substring(sel_dt,6, 2)
            THEN report_date||' '||end_time ELSE left(sel_dt, 7)||'-01 00:00:00' END INTO cvrt_dt
        FROM web.his_time_push
        WHERE report_date<sel_dt
        ORDER BY report_date DESC LIMIT 1;
    END IF;
    -- 取今天报表结束时间(如果当天批量结束，取批量时间，反之取18:00:00)
    IF cvrt_typ='tdy_rpt_end_dtm'
    THEN
        WITH temp AS (
            SELECT
                CASE WHEN substring(report_date,6, 2)=substring(sel_dt,6, 2)
                THEN report_date||' '||end_time ELSE left(report_date, 7)||'-01 00:00:00' END AS cvrt_dt
            FROM web.his_time_push
            WHERE report_date>=sel_dt
            ORDER BY report_date LIMIT 1
        ),
        current_end_time AS (
            SELECT end_time FROM web.params_push
        )
        SELECT
            CASE WHEN b.cvrt_dt IS NULL THEN current_date||' '||a.end_time
                ELSE b.cvrt_dt END INTO cvrt_dt
        FROM current_end_time a
        LEFT JOIN temp b ON 1=1;
    END IF;
    -- 取下一个工作日
    IF cvrt_typ='nxt_workday'
    THEN
        SELECT date INTO cvrt_dt
        FROM portal.hyb_work_day WHERE date>sel_dt AND status='1' ORDER BY date LIMIT 1;
    END IF;
    -- 取季度的第一天
    IF cvrt_typ='quar_start'
    THEN
        WITH dt_temp AS (
            SELECT
                left(sel_dt, 4) AS d_year,
                substring(sel_dt, 6, 2) AS d_month
            )
        SELECT
            CASE WHEN d_month IN ('01', '02', '03') THEN d_year||'-01-01 00:00:00'
                WHEN d_month IN ('04', '05', '06') THEN d_year||'-04-01 00:00:00'
                WHEN d_month IN ('07', '08', '09') THEN d_year||'-07-01 00:00:00'
                ELSE d_year||'-10-01 00:00:00' END INTO cvrt_dt
        FROM dt_temp;
    END IF;
    -- 取下个季度的第一天
    IF cvrt_typ='quar_end'
    THEN
        WITH dt_temp AS (
            SELECT
                left(sel_dt, 4) AS d_year,
                (left(sel_dt, 4)::int+1)::varchar AS next_year,
                substring(sel_dt, 6, 2) AS d_month
            )
        SELECT
            CASE WHEN d_month IN ('01', '02', '03') THEN d_year||'-04-01 00:00:00'
                WHEN d_month IN ('04', '05', '06') THEN d_year||'-07-01 00:00:00'
                WHEN d_month IN ('07', '08', '09') THEN d_year||'-10-01 00:00:00'
                ELSE next_year||'-01-01 00:00:00' END INTO cvrt_dt
        FROM dt_temp;
    END IF;
    -- 取当日业绩表数据时效性时间(如果当天批量结束，取批量时间，反之取当前时间)
    IF cvrt_typ='data_age_dtm'
    THEN
        SELECT
            COALESCE(report_date||' '||end_time,sel_dt) INTO cvrt_dt
        FROM (SELECT 1) b
        LEFT JOIN web.his_time_push a ON report_date>=substring(sel_dt,1,10)
        ORDER BY report_date LIMIT 1;
    END IF;
    RETURN cvrt_dt;
END;
$cvrt_dt$ LANGUAGE plpgsql;

-- 分公司权限
DROP FUNCTION if EXISTS auth_comp (emp_cd varchar(40));
CREATE OR REPLACE FUNCTION auth_comp (emp_cd varchar(40))
RETURNS int[] AS $comp_ids$
declare
    comp_ids int[];
BEGIN
    SELECT ARRAY(
        SELECT
            b.comp_id
        FROM web.auth_emp_to_comp a,dw.dim_comp b
        WHERE auth_cat='all' AND emp_code=emp_cd
        UNION
        SELECT
            comp_id
        FROM web.auth_emp_to_comp
        WHERE auth_cat='own' AND emp_code=emp_cd
    ) INTO comp_ids;
RETURN comp_ids;
END;
$comp_ids$ LANGUAGE plpgsql;

-- 机构权限
DROP FUNCTION if EXISTS auth_org (p_emp_cd varchar(40));
CREATE OR REPLACE FUNCTION auth_org (p_emp_cd varchar(40))
RETURNS int[] AS $org_ids$
declare
    org_ids int[];
BEGIN
    SELECT ARRAY(
        SELECT
            b.org_id
        FROM web.auth_emp_to_org a,dw.dim_org b
        WHERE a.auth_cat='all' AND a.emp_cd=p_emp_cd
        UNION
        SELECT
            a.org_id
        FROM web.auth_emp_to_org a
        WHERE a.auth_cat='own' AND a.emp_cd=p_emp_cd
    ) INTO org_ids;
RETURN org_ids;
END;
$org_ids$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_work_days (start_dt date,end_dt date)
RETURNS int AS
$BODY$
declare
    work_days int;
BEGIN
    select count(1) into work_days from dw.dim_date
    where dt_id between start_dt and end_dt
    and is_work_day='Y';
RETURN work_days;
END;
$BODY$  LANGUAGE plpgsql;

--  身份证15位转18位
CREATE OR REPLACE FUNCTION  id15to18(id_str varchar)
      RETURNS "pg_catalog"."varchar" AS
     $BODY$
    DECLARE
    v_sfz	varchar;
    v_i	integer;
    v_sum	integer;
    v_array1 integer[];
    v_array2 varchar[];
    v_s varchar;
    BEGIN
        v_sfz:=upper(trim(id_str));
        if length(v_sfz)=15 then
            v_sfz:=SUBSTR(id_str, 1, 6) || '19' || SUBSTR(id_str, 7, 9); -- 添加中间值 下面对后一位检验
            v_array1:=array[7,9,10,5,8,4,2,1,6,3,7,9,10,5,8,4,2];
            v_array2:=array['1','0','X','9','8','7','6','5','4','3','2'];
            v_i:=1;
            v_sum:=0;
            loop
                v_s:=substr(v_sfz,v_i,1);
                v_sum:=v_sum + cast(v_s as integer)*v_array1[v_i];
                v_i:=v_i + 1;
                if v_i>17 then
                    exit;
                end if;
            end loop;
            v_sum:=mod(v_sum,11) + 1;
            v_s:=v_array2[v_sum];
            return v_sfz||v_s;
        else
            return v_sfz;
        end if;
    END
    $BODY$
    LANGUAGE plpgsql;
    -- 将字符串强制转化为日期，不能转化成功的则返回null
    CREATE OR REPLACE FUNCTION str_to_date(datestr varchar)
        RETURNS date
    AS $BODY$
    BEGIN
    IF (dateStr IS NULL) THEN
         RETURN NUll;
    END IF;
         PERFORM dateStr::timestamp;
         RETURN dateStr::date;
    EXCEPTION
         WHEN others THEN
         RETURN null;
    END;
    $BODY$ LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION cust_renew_detl (start_dt date, end_dt date)
RETURNS TABLE(
        o_cust_no varchar(12),
        o_emp_cd varchar(30),
        o_rfnd_prod_id varchar(50),     -- 划付产品id
        o_rfnd_able_dt date,            -- 划付日期
        o_rfnd_amt numeric(16,2),       -- 划付金额
        o_cont_amt numeric(16,2),       -- 新交易的单笔合同金额
        o_renew_prod_id varchar(50),    -- 续投产品id
        o_yx_renew_amt numeric(16,2),   -- 续投金额
        o_inc_amt numeric(16,2),        -- 增投金额
        o_entry_dt date,                -- 续投日期
        o_cont_cd varchar(50)           -- 续投的合同编号
        ) AS $$
BEGIN
    RETURN query
    WITH temp_end AS (
        SELECT
            cust_no,
            prod_id,
            rfnd_able_dt,
            sum(rfnd_amt) AS rfnd_amt,-- 客户同一天产品划付金额
            sum(sum(rfnd_amt)) OVER(PARTITION BY cust_no ORDER BY rfnd_able_dt,prod_id) AS rfnd_rng_max,-- 划付金额范围最大值
            sum(sum(rfnd_amt)) OVER(PARTITION BY cust_no ORDER BY rfnd_able_dt,prod_id) - sum(rfnd_amt) AS rfnd_rng_min,-- 划付金额范围最小值
            sum(sum(rfnd_amt)) OVER(PARTITION BY cust_no) AS sum_cust_rfnd_amt       -- 客户总划付
        FROM dw.fact_cust_lqd
        WHERE rfnd_able_dt>=start_dt AND rfnd_able_dt<end_dt
        GROUP BY cust_no, prod_id, rfnd_able_dt
    ),
    temp_renew AS (
        SELECT
            b.cust_no,
            b.prod_id,
            b.cont_amt,-- 单笔合同金额
            b.entry_dt,
            b.cont_cd,
            sum(b.cont_amt) OVER(PARTITION BY b.cust_no ORDER BY b.entry_dt,b.prod_id,b.trans_cd) AS cont_rng_max,-- 合同金额范围最大值
            sum(b.cont_amt) OVER(PARTITION BY b.cust_no ORDER BY b.entry_dt,b.prod_id,b.trans_cd) - cont_amt AS cont_rng_min-- 合同金额范围最小值
        FROM dw.fact_trans_his b
        LEFT JOIN dw.dim_prod c ON b.prod_id=c.prod_id
        WHERE b.trans_src=11 AND b.entry_dt>=start_dt AND b.entry_dt<end_dt
            AND (c.cat_1st_nm!='公募基金' OR (c.cat_1st_nm='公募基金' AND b.trans_type!='0036'))
            AND cust_no IN (SELECT DISTINCT cust_no FROM temp_end)
    )
    SELECT
        m.cust_no,  -- 客户编号
        c.emp_cd,   -- 理财师工号
        m.prod_id AS rfnd_prod_id,  -- 划付产品id
        m.rfnd_able_dt,             -- 划付日期
        m.rfnd_amt,                 -- 划付金额
        n.cont_amt,                 -- 新交易的单笔合同金额
        n.prod_id AS renew_prod_id,  -- 续投产品id
        CASE WHEN m.rfnd_rng_max=m.sum_cust_rfnd_amt AND m.rfnd_rng_max<=n.cont_rng_min THEN 0.00   -- 仅增投,合同范围最小值>=划付范围最大值
             WHEN m.rfnd_rng_max<n.cont_rng_max AND m.rfnd_rng_min<=n.cont_rng_min  THEN m.rfnd_rng_max-n.cont_rng_min  -- 合同范围最大值>划付范围最大值,合同范围最小值>=划付范围最小值
             WHEN m.rfnd_rng_max<n.cont_rng_max AND m.rfnd_rng_min>n.cont_rng_min   THEN m.rfnd_amt                     -- 合同范围最大值>划付范围最大值,合同范围最小值<划付范围最小值
             WHEN m.rfnd_rng_max>=n.cont_rng_max AND m.rfnd_rng_min<=n.cont_rng_min THEN n.cont_amt                     -- 合同范围最大值<=划付范围最大值,合同范围最小值>=划付范围最小值
             WHEN m.rfnd_rng_max>=n.cont_rng_max AND m.rfnd_rng_min>n.cont_rng_min  THEN n.cont_rng_max-m.rfnd_rng_min  -- 合同范围最大值<=划付范围最大值,合同范围最小值<划付范围最小值
             END AS yx_renew_amt,  -- 续投金额
        CASE WHEN m.rfnd_rng_max=m.sum_cust_rfnd_amt AND m.rfnd_rng_max<=n.cont_rng_min THEN n.cont_amt  -- 仅增投,合同范围最小值>=划付范围最大值
             WHEN m.rfnd_rng_max=m.sum_cust_rfnd_amt AND m.rfnd_rng_max<n.cont_rng_max  THEN n.cont_rng_max-m.sum_cust_rfnd_amt -- 部分增投,
             END AS inc_amt,       -- 增投金额
        n.entry_dt,
        n.cont_cd
    FROM temp_end m
    LEFT JOIN temp_renew n ON m.cust_no=n.cust_no
        AND (
            (m.rfnd_rng_max>n.cont_rng_min AND m.rfnd_rng_min<=n.cont_rng_max) -- 有续签
         OR (m.rfnd_rng_max=m.sum_cust_rfnd_amt AND m.sum_cust_rfnd_amt<=n.cont_rng_min) -- 仅增投
        )
    LEFT JOIN dw.dim_cust c ON m.cust_no=c.cust_no;
    -- ORDER BY m.sum_cust_rfnd_amt DESC, m.cust_no, m.rfnd_rng_max, n.cont_rng_max;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION work_days_rng_dts(start_date date, end_date date)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
    declare
        sum_days integer;
    BEGIN
       SELECT count(1) into sum_days FROM dw.dim_date WHERE dt_id BETWEEN start_date AND end_date AND is_work_day='Y';
       RETURN sum_days;
    END;
   $function$

