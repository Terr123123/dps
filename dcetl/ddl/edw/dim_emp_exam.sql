CREATE TABLE edw.dim_emp_exam (
	exam_cd varchar(30) NOT NULL, -- 测试编号
	exam_nm varchar(300) NULL, -- 测试名称
	exam_dtm timestamp NULL, -- 测试时间
	exam_show_nm varchar(300) NULL, -- 测试展示名称
	tot_score int , -- 测试总分
	CONSTRAINT dim_emp_exam_pk PRIMARY KEY (exam_cd)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX dim_emp_exam_exam_dtm_idx ON edw.dim_emp_exam USING btree (exam_dtm) ;
CREATE INDEX dim_emp_exam_exam_show_nm_idx ON edw.dim_emp_exam USING btree (exam_show_nm) ;

-- Column comments
COMMENT ON TABLE edw.dim_emp_exam IS '测试基本信息' ;
COMMENT ON COLUMN edw.dim_emp_exam.exam_cd IS '测试编号' ;
COMMENT ON COLUMN edw.dim_emp_exam.exam_nm IS '测试名称' ;
COMMENT ON COLUMN edw.dim_emp_exam.exam_dtm IS '测试时间' ; -- 接口未提供用，用最先参与考试的测试时间
COMMENT ON COLUMN edw.dim_emp_exam.exam_show_nm IS '测试展示名称' ; --  路演名称 一般是测试的报表的展示名称
COMMENT ON COLUMN edw.dim_emp_exam.tot_score IS '测试总分' ;