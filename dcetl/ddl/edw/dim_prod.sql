 CREATE TABLE edw.dim_prod (
        prod_id varchar(50) NOT NULL, -- 唯一产品id
        src_prod_id varchar(50) NULL, -- 源系统产品id
        src_sys_cd varchar(40) NULL, -- 源系统编号
        prod_nm varchar(200) NULL, -- 产品名称
        prod_cd varchar(40) NULL, -- 产品编码
--         shr_type_nm varchar(100) NULL, -- 份额类别名称
        prod_type varchar(20) NULL, -- 产品大类
        cat_1st_nm varchar(40) NULL, -- 一级分类
        cat_2nd_nm varchar(40) NULL, -- 二级分类
        cat_3rd_nm varchar(40) NULL, -- 三级分类
        prod_alia varchar(40) NULL, -- 产品简称
        prod_risk_lvl varchar(40) NULL, -- 产品风险等级
        raise_amnt numeric(16,2) NULL, -- 募集总额
        prod_esta_dt date NULL, -- 产品成立日期
        raise_beg_dt date NULL, -- 募集开始日期
        raise_end_dt date NULL, -- 募集结束日期
        mgr_nm varchar(40) NULL, -- 产品管理人
        mop_mgr_nm varchar(40) NULL, -- 业绩归属管理人
        esta_stat varchar(40) NULL, -- 成立状态
        online_stat varchar(40) NULL, -- 上架状态
        cstd varchar(100) NULL, -- 托管人
        elec_cont_flag varchar(2) NULL, -- 是否用电子合同
        oper_type varchar(40) NULL, -- 运作方式
        sale_type varchar(40) NULL, -- 销售方式
        crcy_type varchar(40) NULL, -- 货币类型
        fast_redm_flag varchar(10) NULL, -- 快速赎回标识
        raise_stat varchar(10) NULL, -- 募集状态
        cont_cnt int4 NULL, -- 合同数
        expc_onln_dt date NULL, -- 预期上线时间  无效字段,以expc_onln_dtm为准
        invst_dire text NULL, -- 投资方向
        fund_mgr varchar(50) NULL, -- 基金管理人
        prod_beg_dt date NULL, -- 产品起息日期
        prod_end_dt date NULL, -- 产品结束日期
        conf_stat varchar(50) NULL, -- 审批状态
        mgr_group varchar(50) NULL, -- 所属板块
        acc_open_days int8 NULL, -- 累计开放天数
        end_dt_calc_way varchar(20) NULL, -- 到期日计算方式
        expc_onln_dtm timestamp, -- 预约上架时间
        rerd_dtm date, -- 备案日期
        prod_nm_cd varchar(64), -- 名称代码
        last_pay_dt date, -- 最后打款日
        onln_dt date,     -- 产品上线日期(暂时只有公募)
        fst_raise_beg_dt date,    -- 首次募集开始日
        last_raise_end_dt date,   -- 最新募集结束日
        prod_std_kpi numeric(10,4),   -- 产品标准KPI系数
        elec_sign_flag varchar(1) NULL,-- 是否支持电子签署
        prod_proc_tag varchar(50) NULL , -- 产品处理标签
        CONSTRAINT dim_prod_pk PRIMARY KEY (prod_id)
    )
    WITH (
        OIDS=FALSE
    ) ;
    CREATE INDEX dim_prod_src_prod_id_idx ON edw.dim_prod USING btree (src_prod_id, src_sys_cd) ;

    -- Column comments
    COMMENT ON TABLE edw.dim_prod IS '产品信息维度表';
    COMMENT ON COLUMN edw.dim_prod.prod_id IS '唯一产品id' ;
    COMMENT ON COLUMN edw.dim_prod.src_prod_id IS '源系统产品id' ;
    COMMENT ON COLUMN edw.dim_prod.src_sys_cd IS '源系统编号' ;
    COMMENT ON COLUMN edw.dim_prod.prod_nm IS '产品名称' ;
    COMMENT ON COLUMN edw.dim_prod.prod_cd IS '产品编码' ;
--     COMMENT ON COLUMN edw.dim_prod.shr_type_nm IS '份额类别名称' ;
    COMMENT ON COLUMN edw.dim_prod.prod_type IS '产品大类' ;
    COMMENT ON COLUMN edw.dim_prod.cat_1st_nm IS '一级分类' ;
    COMMENT ON COLUMN edw.dim_prod.cat_2nd_nm IS '二级分类' ;
    COMMENT ON COLUMN edw.dim_prod.cat_3rd_nm IS '三级分类' ;
    COMMENT ON COLUMN edw.dim_prod.prod_alia IS '产品简称' ;
    COMMENT ON COLUMN edw.dim_prod.prod_risk_lvl IS '产品风险等级' ;
    COMMENT ON COLUMN edw.dim_prod.raise_amnt IS '募集总额' ;
    COMMENT ON COLUMN edw.dim_prod.prod_esta_dt IS '产品成立日期' ;
    COMMENT ON COLUMN edw.dim_prod.raise_beg_dt IS '募集开始日期' ;
    COMMENT ON COLUMN edw.dim_prod.raise_end_dt IS '募集结束日期' ;
    COMMENT ON COLUMN edw.dim_prod.mgr_nm IS '产品管理人' ;
    COMMENT ON COLUMN edw.dim_prod.mop_mgr_nm IS '业绩归属管理人' ;
    COMMENT ON COLUMN edw.dim_prod.esta_stat IS '成立状态' ;
    COMMENT ON COLUMN edw.dim_prod.online_stat IS '上架状态' ;
    COMMENT ON COLUMN edw.dim_prod.cstd IS '托管人' ;
    COMMENT ON COLUMN edw.dim_prod.elec_cont_flag IS '是否用电子合同' ;
    COMMENT ON COLUMN edw.dim_prod.oper_type IS '运作方式' ;
    COMMENT ON COLUMN edw.dim_prod.sale_type IS '销售方式' ;
    COMMENT ON COLUMN edw.dim_prod.crcy_type IS '货币类型' ;
    COMMENT ON COLUMN edw.dim_prod.fast_redm_flag IS '快速赎回标识' ;
    COMMENT ON COLUMN edw.dim_prod.raise_stat IS '募集状态' ;
    COMMENT ON COLUMN edw.dim_prod.cont_cnt IS '合同数' ;
    COMMENT ON COLUMN edw.dim_prod.expc_onln_dt IS '预期上线时间' ;
    COMMENT ON COLUMN edw.dim_prod.invst_dire IS '投资方向' ;
    COMMENT ON COLUMN edw.dim_prod.fund_mgr IS '基金管理人' ;
    COMMENT ON COLUMN edw.dim_prod.prod_beg_dt IS '产品起息日期' ;
    COMMENT ON COLUMN edw.dim_prod.prod_end_dt IS '产品结束日期' ;
    COMMENT ON COLUMN edw.dim_prod.conf_stat IS '审批状态' ;
    COMMENT ON COLUMN edw.dim_prod.mgr_group IS '所属板块' ;
    COMMENT ON COLUMN edw.dim_prod.acc_open_days IS '累计开放天数' ;
    COMMENT ON COLUMN edw.dim_prod.end_dt_calc_way IS '到期日计算方式' ;
    COMMENT ON COLUMN edw.dim_prod.expc_onln_dtm IS '预约上架时间' ;
    COMMENT ON COLUMN edw.dim_prod.rerd_dtm IS '备案日期' ;
    COMMENT ON COLUMN edw.dim_prod.prod_nm_cd IS '产品名称代码' ;
    COMMENT ON COLUMN edw.dim_prod.last_pay_dt IS '最后打款日' ;
    COMMENT ON COLUMN edw.dim_prod.onln_dt IS '产品上线日期' ;
    COMMENT ON COLUMN edw.dim_prod.fst_raise_beg_dt IS '首次募集开始日' ;
    COMMENT ON COLUMN edw.dim_prod.last_raise_end_dt IS '最新募集结束日' ;
    COMMENT ON COLUMN edw.dim_prod.prod_std_kpi IS '产品标准KPI系数' ;
    COMMENT ON COLUMN edw.dim_prod.elec_sign_flag IS '是否支持电子签署';
    COMMENT ON COLUMN edw.dim_prod.prod_proc_tag IS '产品处理标签';