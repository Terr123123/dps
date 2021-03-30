-- 弹窗频率提醒
CREATE TABLE adl.mapp_msg_ntce (
    sys_flag varchar(20) NOT NULL,          -- 系统标识
    emp_cd varchar(30) NOT NULL,            -- 员工工号
    is_prmt_msg varchar(1) default 'N',     -- 是否弹窗提示信息 默认提示N
    prmt_msg_fqcy varchar(1),               -- 弹窗频率 0:周 1:月
    upd_dtm timestamp NULL DEFAULT now(),    -- 更新时间
    CONSTRAINT mapp_msg_ntce_pk PRIMARY KEY (sys_flag, emp_cd)
)
WITH (
    OIDS=FALSE
) ;
COMMENT ON TABLE adl.mapp_msg_ntce IS '弹窗频率提醒';
COMMENT ON COLUMN adl.mapp_msg_ntce.sys_flag IS '系统标识';  -- fpdp:个人数据平台
COMMENT ON COLUMN adl.mapp_msg_ntce.emp_cd IS '员工工号';
COMMENT ON COLUMN adl.mapp_msg_ntce.is_prmt_msg IS '是否弹窗提示信息';
COMMENT ON COLUMN adl.mapp_msg_ntce.prmt_msg_fqcy IS '弹窗频率'; -- 弹窗频率 0:周 1:月
COMMENT ON COLUMN adl.mapp_msg_ntce.upd_dtm IS '更新时间';