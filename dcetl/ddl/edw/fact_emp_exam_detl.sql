CREATE TABLE edw.fact_emp_exam_detl (
	emp_cd varchar(30) NULL, -- 员工编号
	exam_cd varchar(30) NULL, -- 测试编号
	exam_score int4 NULL, -- 测试得分
	oper_dtm timestamp NULL, -- 操作时间
	emp_nm  varchar(30) NULL,  -- 员工名称
	emp_stat  varchar(30) NULL, -- 员工状态
	emp_cat  varchar(30) NULL,  -- 员工类型（前台/后台）
	comp_id int ,  -- 分公司ID
	is_roadshow varchar(2)  NULL, -- 是否路演
	exam_dtm timestamp   NULL,  -- 考试开始时间（第一位员工参与考试的时间） 只用于增量
	CONSTRAINT fact_emp_exam_detl_pk PRIMARY KEY (emp_cd, exam_cd)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX fact_emp_exam_detl_exam_cd_idx ON edw.fact_emp_exam_detl USING btree (exam_cd) ;
CREATE INDEX fact_emp_exam_detl_oper_dtm_idx ON edw.fact_emp_exam_detl USING btree (oper_dtm) ;
CREATE INDEX fact_emp_exam_detl_exam_dtm_idx ON edw.fact_emp_exam_detl USING btree (exam_dtm) ;

-- Column comments
COMMENT ON TABLE edw.fact_emp_exam_detl IS '海银大学考试明细' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.emp_cd IS '员工编号' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.exam_cd IS '测试编号' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.exam_score IS '测试得分' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.oper_dtm IS '操作时间' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.emp_nm IS '员工名称' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.emp_stat IS '员工状态' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.emp_cat IS '员工类型' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.comp_id IS '分公司ID' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.is_roadshow IS '是否路演' ;
COMMENT ON COLUMN edw.fact_emp_exam_detl.exam_dtm IS '考试开始时间' ;  -- 考试开始时间（第一位员工参与考试的时间） 只用于增量