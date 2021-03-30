delete from dms.fact_emp_exam_stats_by_comp where exam_dtm>=current_date-${p_days} or exam_dtm is null;
insert into dms.fact_emp_exam_stats_by_comp
with tp_comp as(  -- 分公司在职人数
    select portal_comp_id comp_id,count(1) emp_cnt from dw.dim_emp e
    where e.emp_stat<>'离职' and e.emp_cat='前台' group by portal_comp_id),
tp_join as(
     -- 获取每场考试每个公司的参与人数、合格人数 剔除离职员工
    select
          d.comp_id,
          d.exam_cd,
          count(distinct d.emp_cd) emp_join_cnt,
          sum(case when exam_score >= 60 then 1 else 0 end ) qual_cnt
    from edw.fact_emp_exam_detl d
    where d.is_roadshow='Y' and d.exam_dtm>=current_date-${p_days}
    group by d.comp_id,d.exam_cd),
tp_all_comp as(
     -- 分公司和考试的一个交叉信息，作为主信息
    select c.comp_id,e.exam_cd,e.exam_dtm from dw.dim_comp c cross join edw.dim_emp_exam e
    where c.is_actv='Y' and e.exam_dtm>=current_date-${p_days} and c.comp_id>709)
select
    tp_all_comp.comp_id,
    coalesce(tp_comp.emp_cnt,0) emp_cnt,
    tp_all_comp.exam_cd,
    coalesce(tp_join.emp_join_cnt,0) emp_join_cnt,
    coalesce(tp_join.qual_cnt,0) qual_cnt,
    case when tp_comp.emp_cnt>0 then round(coalesce(tp_join.emp_join_cnt,0)*1.0/emp_cnt,4) else 0 end join_rate,
    tp_all_comp.exam_dtm ---- 测试时间 根据参与人员最早的参与时间计算出来 只用来做增量无实际意义
from tp_all_comp
left join tp_comp on tp_all_comp.comp_id=tp_comp.comp_id
left join tp_join on tp_all_comp.comp_id=tp_join.comp_id and tp_all_comp.exam_cd=tp_join.exam_cd;




with tp_join as(
     -- 获取每场考试每个公司的参与人数、合格人数 剔除离职员工
    select
          d.comp_id,
          d.exam_cd,
          count(distinct d.emp_cd) emp_join_cnt,
          sum(case when exam_score >= 60 then 1 else 0 end ) qual_cnt
    from edw.fact_emp_exam_detl d
    where d.is_roadshow='Y' --and d.exam_dtm>=current_date-${p_days}
    group by d.comp_id,d.exam_cd),
tp_comp as(
     -- 分公司和考试的一个交叉信息，作为主信息
    select c.comp_id,e.exam_cd,e.exam_dtm::date roadshow_dt
    from dw.dim_comp c
         cross join edw.dim_emp_exam e
    where c.is_actv='Y' and e.exam_dtm>=current_date-${p_days} and c.comp_id>709
    and not exists(select 1 from dms.fact_emp_exam_stats_by_comp where fact_emp_exam_stats_by_comp.exam_cd=e.exam_cd)
    ),
tp_all_comp as(
   select  c.comp_id,c.exam_cd,count(e.emp_cd) emp_cnt,c.roadshow_dt from tp_comp c left join dw.dim_emp e
   on c.comp_id=e.portal_comp_id  and e.emp_cat='前台' and e.join_dt<=c.roadshow_dt and coalesce(e.left_dt,'2099-01-01')>c.roadshow_dt+7
   group by c.comp_id,c.exam_cd,c.roadshow_dt)
select
    tp_all_comp.comp_id,
    tp_all_comp.emp_cnt emp_cnt,
    tp_all_comp.exam_cd,
    coalesce(tp_join.emp_join_cnt,0) emp_join_cnt,
    coalesce(tp_join.qual_cnt,0) qual_cnt,
    case when tp_all_comp.emp_cnt<=0 then 0
         when tp_join.emp_join_cnt>=tp_all_comp.emp_cnt then 1
         else  round(coalesce(tp_join.emp_join_cnt,0)*1.0/emp_cnt,4)  end join_rate,
    tp_all_comp.roadshow_dt ---- 测试时间 根据参与人员最早的参与时间计算出来 只用来做增量无实际意义
from tp_all_comp
left join tp_join on tp_all_comp.comp_id=tp_join.comp_id and tp_all_comp.exam_cd=tp_join.exam_cd;
   -- 问题 入职日期为多天的才算在分母上
   -- 离职日期录入滞后问题