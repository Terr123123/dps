-- 删除历史数据
delete from edw.fact_emp_exam_detl where exam_dtm>=current_date - ${p_days} or exam_dtm is null;
-- 插入新纪录
insert into edw.fact_emp_exam_detl (emp_cd, exam_cd, exam_score, oper_dtm, emp_nm, emp_stat, emp_cat, comp_id, is_roadshow, exam_dtm)
with tp_inc as (select uno emp_cd,
                       itemcode exam_cd,
                       max(ceshiscore :: int) exam_score,
                       max(ceshitime) oper_dtm,
                       min(e.exam_dtm) exam_dtm -- 主要用途为增量处理
                from ods.elearning_exam_result s
                       inner join edw.dim_emp_exam e
                         on s.itemcode = e.exam_cd
                              and e.exam_dtm >= current_date - ${p_days}
                group by uno,itemcode)
select tp_inc.emp_cd,
       exam_cd,
       exam_score,
       oper_dtm,
m.emp_nm,m.emp_stat,m.emp_cat,c.comp_id,
case when m.emp_stat<>'离职' and m.emp_cat='前台' and c.is_actv='Y' and c.comp_id>709 then 'Y' else 'N' end is_roadshow,
exam_dtm
from tp_inc
left join dw.dim_emp m on tp_inc.emp_cd=m.emp_cd
inner join dw.dim_comp c on m.portal_comp_id=c.comp_id
