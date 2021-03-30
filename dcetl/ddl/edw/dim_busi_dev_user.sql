CREATE TABLE edw.dim_busi_dev_user (
	open_id varchar(64) NOT NULL, -- open_id
	union_id varchar(64) not NULL,  -- 联合统一ID
	wx_nick_nm varchar(64) NULL, -- 微信昵称
	wx_photo_url varchar(256) NULL, -- 微信头像链接
	crm_cust_no varchar(30) NULL, -- CRM客户编号
	emp_no varchar(32) NULL, -- 员工编号
	src_accnt_id varchar(64) NULL, -- 源系统账户ID
	user_type varchar(20) not NULL , -- 用户类型 wechat 微信用户（牛头帮开发的） card 微名片 mp 小程序用户（自己开发的为了区分和牛头帮开发的）
	gender varchar NULL, -- 性别
	city_nm varchar(64) NULL, -- 城市名称
	prov_nm varchar(64) NULL, -- 省份名称
	crty_nm varchar(64) NULL, -- 国家名称
	crt_dtm timestamp NULL, -- 创建时间
  etl_dtm timestamp NULL DEFAULT now(), -- ETL处理时间
	CONSTRAINT dim_busi_dev_user_pk PRIMARY KEY (open_id)
)
WITH (
	OIDS=FALSE
) ;
CREATE UNIQUE INDEX dim_busi_dev_user_accnt_id_idx ON edw.dim_busi_dev_user USING btree (src_accnt_id) ;
CREATE INDEX dim_busi_dev_user_crt_dtm_idx ON edw.dim_busi_dev_user USING btree (crt_dtm) ;
COMMENT ON TABLE  edw.dim_busi_dev_user IS '微信基础信息';
COMMENT ON COLUMN edw.dim_busi_dev_user.open_id IS 'open_id' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.union_id IS '联合统一ID' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.wx_nick_nm IS '微信昵称' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.wx_photo_url IS '微信头像链接' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.crm_cust_no IS 'CRM客户编号' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.emp_no IS '员工编号' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.user_type IS '用户类型' ;
--  -- 用户类型 wechat 微信用户（牛头帮开发的） card 微名片 mp 小程序用户（自己开发的为了区分和牛头帮开发的）
COMMENT ON COLUMN edw.dim_busi_dev_user.src_accnt_id IS '源系统账户ID' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.gender IS '性别' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.city_nm IS '城市名称' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.prov_nm IS '省份名称' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.crty_nm IS '国家名称' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.crt_dtm IS '创建时间' ;
COMMENT ON COLUMN edw.dim_busi_dev_user.etl_dtm IS 'ETL处理时间' ;