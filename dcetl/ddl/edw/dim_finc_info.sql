CREATE TABLE edw.dim_finc_info (
	info_id varchar(50) NOT NULL, -- 资讯主键
	info_nm varchar(200) NULL, -- 资讯名称
	info_url varchar(200) NULL, -- 资讯链接
	info_from varchar(100) NULL, -- 资讯来源
	info_type varchar(50) NULL, -- 资讯类型
	info_pic_url varchar(2000) NULL, -- 图片链接
	info_mini_pic_url varchar(2000) NULL, -- 迷你图片链接
	info_audio_url varchar(2000) NULL, -- 多媒体链接
	info_sum_cntnt varchar(2000) NULL, -- 摘要内容
	vrst_cnt int4 NULL, -- 访问次数
	info_author varchar(100) NULL, -- 资讯作者
	src_sys_info_id varchar(50) NULL, -- 源系资讯编号
	crt_dtm timestamp NULL, -- 创建时间
	pub_dtm timestamp NULL, -- 发布时间
	CONSTRAINT dim_finc_info_pk PRIMARY KEY (info_id)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX dim_finc_info_crt_dtm_idx ON edw.dim_finc_info USING btree (crt_dtm) ;

COMMENT ON TABLE  edw.dim_finc_info IS '财经咨询信息';
COMMENT ON COLUMN edw.dim_finc_info.info_id IS '资讯主键' ;
COMMENT ON COLUMN edw.dim_finc_info.info_nm IS '资讯名称' ;
COMMENT ON COLUMN edw.dim_finc_info.info_url IS '资讯链接' ;
COMMENT ON COLUMN edw.dim_finc_info.info_from IS '资讯来源' ;
COMMENT ON COLUMN edw.dim_finc_info.info_type IS '资讯类型' ;
COMMENT ON COLUMN edw.dim_finc_info.info_pic_url IS '图片链接' ;
COMMENT ON COLUMN edw.dim_finc_info.info_mini_pic_url IS '迷你图片链接' ;
COMMENT ON COLUMN edw.dim_finc_info.info_audio_url IS '多媒体链接' ;
COMMENT ON COLUMN edw.dim_finc_info.info_sum_cntnt IS '摘要内容' ;
COMMENT ON COLUMN edw.dim_finc_info.vrst_cnt IS '访问次数' ;
COMMENT ON COLUMN edw.dim_finc_info.info_author IS '资讯作者' ;
COMMENT ON COLUMN edw.dim_finc_info.src_sys_info_id IS '源系资讯编号' ;
COMMENT ON COLUMN edw.dim_finc_info.crt_dtm IS '创建时间' ;
COMMENT ON COLUMN edw.dim_finc_info.pub_dtm IS '发布时间' ;
