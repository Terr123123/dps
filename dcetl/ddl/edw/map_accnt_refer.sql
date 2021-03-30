CREATE TABLE edw.map_accnt_refer (
	src_sys_cd varchar(20) NULL, -- 源系统编号
	rltn_nm varchar(50) NULL, -- 推荐关系
	accnt_cd varchar(50) NOT NULL, -- 账户编号
	refer_accnt_cd varchar(50) NULL, -- 推荐人账户编号
	crt_dtm timestamp NULL, -- 创建时间
	CONSTRAINT map_accnt_refer_pk PRIMARY KEY (accnt_cd)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX map_accnt_refer_refer_accnt_cd_idx ON edw.map_accnt_refer USING btree (refer_accnt_cd) ;

-- Column comments
COMMENT ON TABLE  edw.map_accnt_refer IS '账户推荐关系';
COMMENT ON COLUMN edw.map_accnt_refer.src_sys_cd IS '源系统编号' ;
COMMENT ON COLUMN edw.map_accnt_refer.rltn_nm IS '推荐关系' ;
COMMENT ON COLUMN edw.map_accnt_refer.accnt_cd IS '账户编号' ;
COMMENT ON COLUMN edw.map_accnt_refer.refer_accnt_cd IS '推荐人账户编号' ;
COMMENT ON COLUMN edw.map_accnt_refer.crt_dtm IS '创建时间' ;
