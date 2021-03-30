CREATE TABLE edw.map_cust_pool (
	cust_no serial NOT NULL, -- 客户编号
	cert_type varchar(10) NOT NULL, -- 证件类型
	cert_cd varchar(50) NOT NULL, -- 证件号
	crt_dtm timestamp null DEFAULT now(), -- 创建时间
	CONSTRAINT map_cust_pool_pk PRIMARY KEY (cert_cd, cert_type)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX map_cust_pool_cust_no_idx ON edw.map_cust_pool USING btree (cust_no) ;

-- Column comments
COMMENT ON table edw.map_cust_pool is '客户证件映射池';
COMMENT ON COLUMN edw.map_cust_pool.cust_no IS '客户编号' ;
COMMENT ON COLUMN edw.map_cust_pool.cert_type IS '证件类型' ;
COMMENT ON COLUMN edw.map_cust_pool.cert_cd IS '证件号' ;
COMMENT ON COLUMN edw.map_cust_pool.crt_dtm IS '创建时间' ;