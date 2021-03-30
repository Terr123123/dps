# encoding: utf-8
"""
员工角色映射表

Created on 20-01-13

@author: xuzh

times : 每天处理全量
"""

import logging
from dpsetl.comm.sqlcomm import exec_sql, upd_tb_strct
from dpsetl.comm.dbutils import get_conn

logger = logging.getLogger(__name__)
tb_nm = 'edw.map_emp_to_role'
tb_nm_cn = "员工角色映射表"
func_name = tb_nm_cn
proc_all_flag = False  # 全量处理
# 表定义sql
ddl_sql = """
   CREATE TABLE edw.map_emp_to_role (
        src_sys_cd varchar(20) NOT NULL, -- 源系统编号
        src_role_id varchar(10) NOT NULL, -- 源系统角色ID
        emp_cd varchar(64) NOT NULL, -- 员工号
        role_nm varchar(64) NULL, -- 角色名称
        CONSTRAINT map_emp_to_role_pk PRIMARY KEY (emp_cd, src_role_id)
    )
    WITH (
        OIDS=FALSE
    ) ;
    
    -- Column comments
    COMMENT ON TABLE  edw.map_emp_to_role IS '员工角色映射表';
    COMMENT ON COLUMN edw.map_emp_to_role.src_sys_cd IS '源系统编号' ;
    COMMENT ON COLUMN edw.map_emp_to_role.src_role_id IS '源系统角色ID' ;
    COMMENT ON COLUMN edw.map_emp_to_role.emp_cd IS '员工号' ;
    COMMENT ON COLUMN edw.map_emp_to_role.role_nm IS '角色名称' ;
"""


def map_emp_to_role(conn):
    """
    :param conn:
    :return:
    """
    logger.info('start %s' % func_name)
    sql = """
    TRUNCATE TABLE edw.map_emp_to_role;
    insert into edw.map_emp_to_role(
        src_sys_cd  , -- 源系统编号
        src_role_id , -- 源系统角色ID
        emp_cd   , -- 员工号
        role_nm   -- 角色名称
    )
    select 
    'portal' src_sys_cd, 
    b.role_id role_id,
    a.user_name AS emp_cd, 
    c.role_name role_nm
    FROM portal.hyb_user a
    LEFT JOIN portal.hyb_role_relation  b ON a.id=b.object_id AND b.active_flag='y'
    LEFT JOIN portal.hyb_role           c ON b.role_id=c.id
    WHERE b."type"='user'  and a.user_name>'' and b.role_id >0
    UNION 
    SELECT  
    'portal' src_sys_cd, 
    rr.role_id,
    u.user_name AS emp_cd, 
    r.role_name
    FROM portal.hyb_role_relation rr 
    LEFT JOIN portal.hyb_role r on rr.role_id = r.id
    LEFT JOIN portal.hyb_position p on rr.object_id = p.ID
    LEFT JOIN portal.hyb_emp_position ep on ep.position_id = p.id AND ep.active_flag='y'
    LEFT JOIN portal.hyb_user_emp ue on ue.emp_no = ep.emp_no
    LEFT JOIN portal.hyb_user u on ue.user_id = u.id
    WHERE rr.type = 'position' AND rr.active_flag='y' and u.user_name>'' and rr.role_id>0
    """
    exec_sql(conn, sql)
    conn.commit()
    logger.info('... end %s' % func_name)


def deal():
    """
    处理入口
    :return:
    """
    conn = get_conn()
    try:
        with conn:
            if upd_tb_strct(conn, crt_tb_sql=ddl_sql, schm_tb_nm=tb_nm, drop_direct=proc_all_flag):
                map_emp_to_role(conn)
    finally:
        conn.close()


if __name__ == '__main__':
    deal()
