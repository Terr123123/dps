 CREATE TABLE stg.stg_dim_cust (
        cust_no varchar(20) NOT NULL, -- 客户号
        cust_nm varchar(150) NULL, -- 客户名称
        cert_type varchar(10) null, -- 证件类型
        cert_cd varchar(50) null ,  -- 证件号
        cust_show_nm varchar(100) NULL, -- 客户展示名称
        is_org varchar(1) NULL, -- 是否机构
        cust_sex varchar(2) NULL, -- 客户性别
        emp_cd varchar(32) NULL, -- 归属理财师编号
        comp_id int4 NULL, -- 归属分公司编号
        mbr_stat varchar(50) NULL, -- 会员状态
        mbr_admit_dt date NULL, -- 入会日期
        -- cont_cert_type varchar(50) NULL, -- 合同认证方式
        -- cust_grade varchar(10) NULL, -- 客户等级  需要计算的 放在客户统计里面
        -- mbr_status varchar(50) NULL, -- 会员状态
        mbr_no varchar(50) NULL, -- 会员编号
        cust_bday date NULL, -- 出生日期
        ref_cust_no varchar(20) NULL, -- 推荐人
        crm_cust_id varchar(20) NULL, -- CRM客户ID
        is_emp varchar(1) NULL, -- 是否公司员工
        crt_dtm timestamp NULL, -- 创建时间
        -- cust_grade_nm varchar(50) NULL, -- 客户等级名称  需要计算的 放在客户统计里面
        -- etl_upd_dtm timestamp NULL default current_timestamp, -- ETL数据更新时间
        -- cust_status varchar(50) NULL, -- 客户状态   存续、机会、休眠 需要计算的 放在客户统计里面
        cust_from varchar(30) NULL,    -- 客户来源
        cust_risk_lvl varchar(50) NULL, -- 风险等级
        crm_cust_old_cd varchar(20) NULL, -- CRM客户老编号
        src_sys_cd varchar(128) NULL, -- 源系统编号
        CONSTRAINT stg_dim_cust_pk PRIMARY KEY (cust_no)
    )
    WITH (
        OIDS=FALSE
    ) ;
--       CREATE INDEX stg_dim_cust_comp_id_idx ON stg.stg_dim_cust USING btree (comp_id) ;
--       CREATE INDEX stg_dim_cust_emp_cd_idx ON stg.stg_dim_cust USING btree (emp_cd) ;
    -- comments
    COMMENT ON TABLE stg.stg_dim_cust IS '客户信息中间表' ;
    COMMENT ON COLUMN stg.stg_dim_cust.cust_no IS '客户号' ;
    COMMENT ON COLUMN stg.stg_dim_cust.cust_nm IS '客户名称' ;
    COMMENT ON COLUMN stg.stg_dim_cust.cert_type IS '证件类型' ;
    COMMENT ON COLUMN stg.stg_dim_cust.cert_cd IS '证件号' ;
    COMMENT ON COLUMN stg.stg_dim_cust.cust_show_nm IS '客户展示名称' ;
    COMMENT ON COLUMN stg.stg_dim_cust.is_org IS '是否机构' ;
    COMMENT ON COLUMN stg.stg_dim_cust.cust_sex IS '客户性别' ;
    COMMENT ON COLUMN stg.stg_dim_cust.emp_cd IS '归属理财师编号' ;
    COMMENT ON COLUMN stg.stg_dim_cust.comp_id IS '归属分公司编号' ;
    COMMENT ON COLUMN stg.stg_dim_cust.mbr_stat IS '会员状态' ;
    COMMENT ON COLUMN stg.stg_dim_cust.mbr_admit_dt IS '入会日期' ;
    -- COMMENT ON COLUMN stg.stg_dim_cust.cont_cert_type IS '合同认证方式' ;
    -- COMMENT ON COLUMN stg.stg_dim_cust.cust_grade IS '客户等级' ;
    -- COMMENT ON COLUMN stg.stg_dim_cust.mbr_status IS '会员状态' ;
    COMMENT ON COLUMN stg.stg_dim_cust.mbr_no IS '会员编号' ;
    COMMENT ON COLUMN stg.stg_dim_cust.cust_bday IS '出生日期' ;
    COMMENT ON COLUMN stg.stg_dim_cust.ref_cust_no IS '推荐人' ;
    COMMENT ON COLUMN stg.stg_dim_cust.crm_cust_id IS 'CRM客户ID' ;
    COMMENT ON COLUMN stg.stg_dim_cust.is_emp IS '是否公司员工' ;
    COMMENT ON COLUMN stg.stg_dim_cust.crt_dtm IS '创建时间' ;
    -- COMMENT ON COLUMN stg.stg_dim_cust.cust_grade_nm IS '客户等级名称' ;
    -- COMMENT ON COLUMN stg.stg_dim_cust.cust_status IS '客户状态' ;
    -- COMMENT ON COLUMN stg.stg_dim_cust.etl_upd_dtm IS 'ETL数据更新时间' ;
    COMMENT ON COLUMN stg.stg_dim_cust.cust_from IS '客户来源' ;
    COMMENT ON COLUMN stg.stg_dim_cust.cust_risk_lvl IS '客户风险等级' ;
    COMMENT ON COLUMN stg.stg_dim_cust.crm_cust_old_cd IS 'CRM客户老编号' ;
    COMMENT ON COLUMN stg.stg_dim_cust.src_sys_cd IS '源系统编号' ;