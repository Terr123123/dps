CREATE TABLE dms.fact_emp_exam_stats_by_comp (
	comp_id int4 NOT NULL, -- 分公司ID
	emp_cnt int8 NULL, -- 分公司在职人数
	exam_cd varchar(30) NOT NULL, -- 测试编号
	emp_join_cnt int8 NULL, -- 参与测试人数
	qual_cnt int8 NULL, -- 合格人数
	join_rate numeric(8,4) NULL, -- 参与比率
	exam_dtm timestamp , -- 测试时间 根据参与人员最早的参与时间计算出来 只用来做增量无实际意义
	CONSTRAINT fact_emp_exam_stats_by_comp_pk PRIMARY KEY (comp_id, exam_cd)
)
WITH (
	OIDS=FALSE
) ;

-- Column comments
COMMENT ON TABLE dms.fact_emp_exam_stats_by_comp IS '员工测试结果按分公司统计' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_comp.comp_id IS '分公司ID' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_comp.emp_cnt IS '分公司在职人数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_comp.exam_cd IS '测试编号' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_comp.emp_join_cnt IS '参与测试人数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_comp.qual_cnt IS '合格人数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_comp.join_rate IS '参与比率' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_comp.exam_dtm IS '测试时间' ;