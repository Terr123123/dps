delete from edw.dim_emp_exam where exam_dtm>=current_date-${p_days};
insert into edw.dim_emp_exam (exam_cd, exam_nm, exam_show_nm, exam_dtm,tot_score)
with tp_inc as(
    select itemcode       exam_cd, -- 测试编码
       max(ceshiname) as exam_nm,  -- 测试名称目前没用
       max(itemname)  exam_show_nm,-- 路演名称 一般是测试的报表的展示名称
       min(ceshitime) exam_dtm  -- 接口未提供用，用最先参与考试的测试时间
from ods.elearning_exam_result where ceshitime>=current_date-${p_days}
group by itemcode)
select tp_inc.exam_cd, -- 测试编码
       tp_inc.exam_nm,  -- 测试名称目前没用
       tp_inc.exam_show_nm,-- 路演名称 一般是测试的报表的展示名称
       tp_inc.exam_dtm,  -- 接口未提供用，用最先参与考试的测试时间
       100 tot_score  -- 协商值100分，有其他分数的不处理
from tp_inc
       left join edw.dim_emp_exam d on tp_inc.exam_cd=d.exam_cd
where d.exam_nm is null;
