# encoding: utf-8
"""
证件映射表

Created on 20-07-20

@author: xuzh

times : 每天处理全量
"""

import logging
from dpsetl.comm.conn import Conn
# from dpsetl import log_datadeal
logger = logging.getLogger()

tb_nm = 'cust.cust_no_pool'
tb_nm_cn = "证件映射表"
func_name = tb_nm_cn
proc_all_flag = False
# 表定义sql
crt_sql = """
            CREATE TABLE cust.cust_no_pool (
                id int8 NOT NULL, -- ID
                cust_no varchar(12) NULL, -- 客户号
                cust_type varchar(1) NOT NULL, -- 客户类型
                cert_type varchar(2) NOT NULL, -- 证件类型
                cert_code varchar(64) NOT NULL, -- 证件号码
                emp_cd  varchar(10) NULL, --员工编号
                crt_dtm timestamp NULL DEFAULT now(),
                CONSTRAINT cust_no_pool_pk PRIMARY KEY (cert_code, cert_type)
            )
            WITH (
                OIDS=FALSE
            ) ;
            CREATE INDEX cust_no_pool_cust_no_idx ON cust.cust_no_pool USING btree (cust_no) ;
            COMMENT ON TABLE cust.cust_no_pool IS '证件映射表' ;
            COMMENT ON COLUMN cust.cust_no_pool.id IS 'ID' ;
            COMMENT ON COLUMN cust.cust_no_pool.cust_no IS '客户号' ;
            COMMENT ON COLUMN cust.cust_no_pool.cust_type IS '客户类型' ;
            COMMENT ON COLUMN cust.cust_no_pool.cert_type IS '证件类型' ;
            COMMENT ON COLUMN cust.cust_no_pool.cert_code IS '证件号码' ;
            COMMENT ON COLUMN cust.cust_no_pool.emp_cd IS '员工编号' ;
            COMMENT ON COLUMN cust.cust_no_pool.crt_dtm IS '创建时间' ;
        """


def cust_no_pool(conn):
    """
    :param conn:
    :return:
    """
    logger.info('start %s' % func_name)
    sql = """INSERT INTO cust.cust_no_pool(
         id,
         cust_no,
         cust_type,
         cert_type,
         cert_code,
         emp_cd
        )
    with emp as (select cert_cd,cert_type_cd cert_type,max(emp_cd) emp_cd  from dw.dim_emp group by cert_cd,cert_type_cd  ),
        temp_cust_cert as(
        select
              distinct
              case when t.is_org='Y' then '0' else '1' end cust_type,
               t.cert_type,
               t.cert_cd,
               emp.emp_cd
        from edw.stg_dim_accnt t
        left join cust.cust_no_pool p on p.cert_code=t.cert_cd and p.cert_type=t.cert_type
        left join emp on t.cert_cd=emp.cert_cd and emp.cert_type=t.cert_type
        where t.cert_cd>'' and t.cert_type>'' and p.cert_code is null
    )
    SELECT
        nextval('cust.seq_cust_id') AS id,
        CASE a.cust_type WHEN '0' THEN '10'||nextval('cust.seq_cust_no')
        ELSE '11'||nextval('cust.seq_cust_no') END AS cust_no,
        a.cust_type,
        a.cert_type,
        a.cert_cd,
        emp_cd
        FROM temp_cust_cert a;"""
    conn.exec(sql=sql)
    # 对历史数据更新emp_cd
    upd_sql = """ with emp as (select cert_cd,cert_type_cd cert_type,max(emp_cd) emp_cd  
                         from dw.dim_emp group by cert_cd,cert_type_cd),
                  upd as(
                   select c.cust_no,emp.emp_cd from  cust.cust_no_pool c left join emp 
                        on c.cert_type=emp.cert_type and c.cert_code=emp.cert_cd
                       where c.emp_cd is null and emp.emp_cd>'')
             update cust.cust_no_pool set emp_cd=upd.emp_cd 
             from upd where upd.cust_no=cust_no_pool.cust_no and cust_no_pool.emp_cd is null;"""
    conn.exec(sql=upd_sql)
    logger.info('end %s' % func_name)


def deal():
    """
    处理入口
    :return:
    """
    conn = Conn()
    try:
        with conn:
            if conn.upd_tb_strct(crt_sql, schm_tb_nm=tb_nm):
                cust_no_pool(conn)
    finally:
        conn.close()


if __name__ == '__main__':
    deal()
