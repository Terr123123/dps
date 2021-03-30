CREATE TABLE rtp.crm_hy_cust_info (
	id int4 NULL, -- id
	old_custid varchar(20) NULL, -- 历史客户编号
	member_no varchar(20) NULL, -- 会员编号
	cust_name varchar(64) NULL, -- 客户名称
	controller varchar(64) NULL, -- controller
	beneficiary varchar(64) NULL, -- beneficiary
	cust_class bpchar NULL, -- 客户类别 01- 个人 02- 机构
	cust_type bpchar NULL, -- 客户类型 01-正式 02-潜在 03-会员
	cert_type bpchar NULL, -- 证件类型 00-身份证  01-护照  02-军官证  04-港澳居民来往内地通行证  0A-台胞证  10-组织机构代码证  11-营业执照 07-三证合一 08-其他
	cert_code varchar(64) NULL, -- 证件号码
	value_date date NULL, -- 证件生效日
	expiry_date date NULL, -- 证件失效日
	mem_auth_mode bpchar NULL, -- 会员认证方式
	ct_auth_mode bpchar NULL, -- 01-客服人员回访 02-自行拨打全国客服热线 03-短信接收 04-电子邮件接收
	accept_mode bpchar NULL, -- 存续资料接收方式 01-电邮接收 02-寄送到家 03-理财师转交
	secret bpchar NULL, -- 财务是否家庭保密 01-是 02-否 默认01
	risk_level varchar(2) NULL, -- 风险偏好 01-保守型 02-稳健型 03-积极型
	cust_source bpchar NULL, -- 客户来源 01- 理财师自建 02- 400来电 03- 微信 04- 官网  05- 基金  06-其他
	cur_status bpchar NULL, -- 状态 01-正常  02-入会审批中  03-信息变更审批中
	admit_time timestamp NULL, -- 入会时间
	is_badrecord bpchar NULL, -- 有误不良记录 01-无 02-有 默认01
-- 	record_desc varchar(300) NULL, -- 不良记录描述
	owner_code varchar(8) NULL, -- 所属理财师
	input_mode bpchar NULL, -- 录入模式 01- 手动 02- 电子
	create_by varchar(8) NULL, -- 创建人
	create_time timestamp NULL, -- 创建时间
-- 	update_by varchar(8) NULL, -- 最后更新人
	update_time timestamp NULL, -- 最后跟新时间
	org_owner varchar(2) NULL, -- 机构归属
-- 	source_desc varchar(255) NULL, -- 来源描述
-- 	share_org varchar(255) NULL, -- 共享机构
	is_controller varchar(2) NULL, -- is_controller
	is_benefit_self varchar(2) NULL, -- is_benefit_self
	is_back varchar(2) NULL, -- 是否审批驳回
-- 	name_pre varchar(2) NULL, -- name_pre
-- 	cust_point int4 NULL, -- 当前积分
-- 	cur_grade varchar(2) NULL, -- 客户等级
	exist_status varchar(2) NULL, -- 存续状态
-- 	can_visit_time timestamp NULL, -- 适宜回访时间
-- 	is_paper varchar(2) NULL, -- 是否订阅晨晚报01是02否
-- 	is_diamond_family varchar(2) NULL, -- 是否是钻石家庭会员01-是 02-否 默认02
-- 	cust_business_flag varchar(2) NULL, -- 是否限制交易01是02否
	mobile_location varchar(255) NULL -- 号码归属地
)
WITH (
	OIDS=FALSE
) ;

COMMENT ON TABLE  rtp.crm_hy_cust_info is 'CRM客户信息表';
COMMENT ON COLUMN rtp.crm_hy_cust_info.id IS 'id' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.old_custid IS '历史客户编号' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.member_no IS '会员编号' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.cust_name IS '客户名称' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.controller IS 'controller' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.beneficiary IS 'beneficiary' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.cust_class IS '客户类别 01- 个人 02- 机构' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.cust_type IS '客户类型 01-正式 02-潜在 03-会员' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.cert_type IS '证件类型' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.cert_code IS '证件号码' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.value_date IS '证件生效日' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.expiry_date IS '证件失效日' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.mem_auth_mode IS '会员认证方式' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.ct_auth_mode IS '01-客服人员回访 02-自行拨打全国客服热线 03-短信接收 04-电子邮件接收' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.accept_mode IS '存续资料接收方式 01-电邮接收 02-寄送到家 03-理财师转交' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.secret IS '财务是否家庭保密 01-是 02-否 默认01' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.risk_level IS '风险偏好 01-保守型 02-稳健型 03-积极型' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.cust_source IS '客户来源 01- 理财师自建 02- 400来电 03- 微信 04- 官网  05- 基金  06-其他' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.cur_status IS '状态 01-正常  02-入会审批中  03-信息变更审批中' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.admit_time IS '入会时间' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.is_badrecord IS '有误不良记录 01-无 02-有 默认01' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.owner_code IS '所属理财师' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.input_mode IS '录入模式 01- 手动 02- 电子' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.create_by IS '创建人' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.create_time IS '创建时间' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.update_by IS '最后更新人' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.update_time IS '最后跟新时间' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.org_owner IS '机构归属' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.source_desc IS '来源描述' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.share_org IS '共享机构' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.is_controller IS 'is_controller' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.is_benefit_self IS 'is_benefit_self' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.is_back IS '是否审批驳回' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.name_pre IS 'name_pre' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.cust_point IS '当前积分' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.cur_grade IS '客户等级' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.exist_status IS '存续状态' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.can_visit_time IS '适宜回访时间' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.is_paper IS '是否订阅晨晚报01是02否' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.is_diamond_family IS '是否是钻石家庭会员01-是 02-否 默认02' ;
-- COMMENT ON COLUMN rtp.crm_hy_cust_info.cust_business_flag IS '是否限制交易01是02否' ;
COMMENT ON COLUMN rtp.crm_hy_cust_info.mobile_location IS '号码归属地' ;


