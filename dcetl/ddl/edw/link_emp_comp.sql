CREATE TABLE edw.link_emp_comp (
        emp_cd varchar(30) NOT NULL, -- 员工编号
        portal_comp_id int8 NOT NULL, -- 人事归属分公司
        comp_id int8 NOT NULL, -- 业绩归属分公司
        start_dt date NOT NULL, -- 有效开始日期
        end_dt date NOT NULL, -- 有效结束日期
        crt_dtm timestamp NULL DEFAULT now(),
        upd_dtm timestamp NULL DEFAULT now(),
        CONSTRAINT link_emp_comp_pk PRIMARY KEY (emp_cd, start_dt)
    )
    WITH (
        OIDS=TRUE
    ) ;
    CREATE INDEX link_emp_comp_end_date ON edw.link_emp_comp USING btree (end_dt) ;
    COMMENT ON TABLE edw.link_emp_comp is  '员工归属分公司拉链表';
    COMMENT ON COLUMN edw.link_emp_comp.emp_cd IS '员工编号' ;
    COMMENT ON COLUMN edw.link_emp_comp.portal_comp_id IS '人事归属分公司' ;
    COMMENT ON COLUMN edw.link_emp_comp.comp_id IS '业绩归属分公司' ;
    COMMENT ON COLUMN edw.link_emp_comp.start_dt IS '有效开始日期' ;
    COMMENT ON COLUMN edw.link_emp_comp.end_dt IS '有效结束日期' ;
    COMMENT ON COLUMN edw.link_emp_comp.crt_dtm IS '创建时间' ;
    COMMENT ON COLUMN edw.link_emp_comp.upd_dtm IS '更新时间' ;