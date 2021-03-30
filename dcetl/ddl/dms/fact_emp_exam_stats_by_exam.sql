CREATE TABLE dms.fact_emp_exam_stats_by_exam (
	exam_cd varchar(30) NULL, -- 测试编号
	emp_join_cnt int8 NULL, -- 参与人数
	avg_score numeric(4) NULL, -- 平均分数
	max_score int4 NULL, -- 最高分数
	min_score int4 NULL, -- 最低分数
	qual_score int4 NULL, -- 合格分数
	qual_cnt int8 NULL, -- 合格人数
	score90_to_100_cnt int8 NULL, -- 得分90-100人数
	score80_to_89_cnt int8 NULL, -- 得分80-89人数
	score70_to_79_cnt int8 NULL, -- 得分70-79人数
	score60_to_69_cnt int8 NULL, -- 得分60-69人数
	score_under60_cnt int8 NULL, -- 得分0-59人数
	CONSTRAINT fact_emp_exam_stats_by_exam_pk PRIMARY KEY (exam_cd)
)
WITH (
	OIDS=FALSE
) ;

-- Column comments
COMMENT ON TABLE dms.fact_emp_exam_stats_by_exam IS '员工测试结果按测试统计' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.exam_cd IS '测试编号' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.emp_join_cnt IS '参与人数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.avg_score IS '平均分数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.max_score IS '最高分数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.min_score IS '最低分数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.qual_score IS '合格分数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.qual_cnt IS '合格人数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.score90_to_100_cnt IS '得分90-100人数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.score80_to_89_cnt IS '得分80-89人数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.score70_to_79_cnt IS '得分70-79人数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.score60_to_69_cnt IS '得分60-69人数' ;
COMMENT ON COLUMN dms.fact_emp_exam_stats_by_exam.score_under60_cnt IS '得分0-59人数' ;