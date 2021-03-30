CREATE TABLE edw.dim_camp (
    camp_id varchar(20) NULL, -- 活动ID
    src_camp_id int NULL, -- 源系统活动ID
    src_sys_cd varchar(10) not  null , -- 源系统编号
    camp_nm varchar(200) NULL, -- 活动主题
    camp_type varchar(100) NULL, -- 活动类型
    start_dtm timestamp NULL, -- 开始时间
    camp_tag varchar(100) NULL, -- 活动标签
    lau_comp_id int NULL, -- 活动发起公司
    crt_dtm timestamp , -- 创建时间
    CONSTRAINT dim_camp_pk PRIMARY KEY (camp_id)
)
WITH (
    OIDS=FALSE
) ;
CREATE INDEX dim_camp_start_dtm_idx ON edw.dim_camp USING btree (start_dtm) ;
CREATE INDEX dim_camp_crt_dtm_idx ON edw.dim_camp USING btree (crt_dtm) ;
COMMENT ON TABLE edw.dim_camp IS '线下活动信息' ;
-- Column comments
COMMENT ON COLUMN edw.dim_camp.camp_id IS '活动ID' ;
COMMENT ON COLUMN edw.dim_camp.src_camp_id IS '源系统活动ID' ;
COMMENT ON COLUMN edw.dim_camp.src_sys_cd IS '源系统编号' ;
COMMENT ON COLUMN edw.dim_camp.camp_nm IS '活动主题' ;
COMMENT ON COLUMN edw.dim_camp.camp_type IS '活动类型' ;
COMMENT ON COLUMN edw.dim_camp.start_dtm IS '开始时间' ;
COMMENT ON COLUMN edw.dim_camp.camp_tag IS '活动标签' ;
COMMENT ON COLUMN edw.dim_camp.lau_comp_id IS '活动发起公司' ;
COMMENT ON COLUMN edw.dim_camp.crt_dtm IS '创建时间' ;