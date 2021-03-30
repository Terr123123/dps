 CREATE TABLE edw.dim_prod_shr_type (
        prod_shr_type_id varchar(20) NOT NULL, -- 份额类别ID
        src_shr_type_id varchar(20) NULL, -- 源系统份额类别ID
        prod_id varchar(50) NULL, -- 产品ID
        shr_type_cd varchar(128) NULL, -- 份额类别编码
        shr_type_nm varchar(64) NULL, -- 份额类别名称
        pay_intst_type varchar(100) NULL, -- 付息方式
        prft_calc_type varchar(100) NULL, -- 收益计算方式
        kpi numeric(10,4) NULL, -- kpi基数
        std_kpi numeric(10,4) NULL, -- std_kpi基数
        is_stn_flag varchar(1) NULL, -- 永续标识
        prod_shr_end_dt date NULL, -- 产品份额到期日
        expc_expr_dt date NULL, -- 预期到期日
        real_expr_dt date NULL, -- 实际到期日
        term_unit varchar(100) NULL, -- 期限类型
        min_ddl varchar(10) NULL, -- 最小投资期限
        max_ddl varchar(10) NULL, -- 最大投资期限
        invst_term_tag varchar(50), -- 投资期限标签
        open_all_fp char(1), -- 是否开放所有理财师
        src_sys_cd varchar(10), -- 源系统编号
        CONSTRAINT dim_prod_shr_type_pk PRIMARY KEY (prod_shr_type_id)
    )
    WITH (
        OIDS=FALSE
    ) ;
    CREATE INDEX dim_prod_shr_type_prod_id_idx ON edw.dim_prod_shr_type (prod_id,prod_shr_type_id) ;
    COMMENT ON TABLE edw.dim_prod_shr_type IS '产品份额类别维度表';
    COMMENT ON COLUMN edw.dim_prod_shr_type.prod_shr_type_id IS '份额类别ID' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.src_shr_type_id IS '源系统份额类别ID' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.prod_id IS '产品ID' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.shr_type_cd IS '份额类别编码' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.shr_type_nm IS '份额类别名称' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.pay_intst_type IS '付息方式' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.prft_calc_type IS '收益计算方式' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.kpi IS 'kpi基数' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.std_kpi IS '标准业绩kpi系数' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.is_stn_flag IS '永续标识' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.prod_shr_end_dt IS '产品份额到期日' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.expc_expr_dt IS '预期到期日' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.real_expr_dt IS '实际到期日' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.term_unit IS '期限类型' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.min_ddl IS '最小投资期限' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.max_ddl IS '最大投资期限' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.invst_term_tag IS '投资期限标签' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.open_all_fp IS '是否开放所有理财师' ;
    COMMENT ON COLUMN edw.dim_prod_shr_type.src_sys_cd IS '源系统编号' ;