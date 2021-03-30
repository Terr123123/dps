#!/usr/bin/env python3.4
# encoding: utf-8
"""
Created on 19-12-22
@title: 'etl_meta_el数据导入配置'
@author: Xuzh
"""
import os
import logging
from etls.comm.conn import Conn
from etls.conf.settings import data_file_path,default_db_cd
from etls.comm.dataxy import crt_el_log_tb

logger = logging.getLogger(__name__)
tb_nm = 'public.etl_meta_el'
tb_nm_cn = "etl_meta_el数据导入配置"
func_name = tb_nm_cn
proc_all_flag = True
data_file_path = data_file_path
ddl_sql = """
        CREATE TABLE public.etl_meta_el (
                task_id serial NOT NULL, -- 任务ID
                tb_nm varchar(100) NOT NULL, -- 目标表名
                tb_nm_cn varchar(100) NULL, -- 表中文含义
                src_tb_nm varchar(100) NOT NULL, -- 源表名
                src_db_cd varchar(30) NOT NULL, -- 源系统连接编码 
                el_type varchar(30) NOT NULL DEFAULT 'datax'::character varying, -- 数据导入类别
                trgt_db_cd varchar(30) NULL DEFAULT 'DPS'::character varying, -- 目标系统编码
                sql_where varchar(2000) NULL, -- sql数据过滤
                sql_pre_sql varchar(2000) NULL, -- sql处理前执行sql语句
                parallel_num numeric(4) NULL DEFAULT 2, -- datax导入进程数
                is_del varchar(1) NULL DEFAULT 'N'::character varying, -- 是否删除
                is_inc varchar(1) NULL DEFAULT 'N'::character varying, -- 是否增量处理
                ignore_cols varchar(500) NULL, -- 忽略字段
                batch_nm varchar(100)  NULL DEFAULT 'T1'::character varying, -- batch_nm 批次名称 用于指定在某个批次上执行
                -- crt_dt date NULL, -- 创建日期
                CONSTRAINT etl_meta_el_pk PRIMARY KEY (tb_nm)
            )
            WITH (
                OIDS=FALSE
            ) ;
            -- Column comments
            comment on table public.etl_meta_el is 'ods同步表信息';
            COMMENT ON COLUMN public.etl_meta_el.task_id IS '任务ID' ;
            COMMENT ON COLUMN public.etl_meta_el.tb_nm IS '目标表名' ;
            COMMENT ON COLUMN public.etl_meta_el.tb_nm_cn IS '表中文含义' ;
            COMMENT ON COLUMN public.etl_meta_el.src_tb_nm IS '源表名' ;
            COMMENT ON COLUMN public.etl_meta_el.src_db_cd IS '源系统连接编码' ;
            COMMENT ON COLUMN public.etl_meta_el.el_type IS '数据导入类别' ; -- 只能是datax pypd
            COMMENT ON COLUMN public.etl_meta_el.trgt_db_cd IS '目标系统编码' ;
            COMMENT ON COLUMN public.etl_meta_el.sql_where IS 'sql数据过滤' ;
            COMMENT ON COLUMN public.etl_meta_el.sql_pre_sql IS 'sql处理前执行sql语句' ;
            COMMENT ON COLUMN public.etl_meta_el.parallel_num IS 'datax导入进程数' ;
            COMMENT ON COLUMN public.etl_meta_el.is_del IS '是否删除' ;
            COMMENT ON COLUMN public.etl_meta_el.is_inc IS '是否增量处理' ;
            COMMENT ON COLUMN public.etl_meta_el.batch_nm IS '指定批次' ;
            COMMENT ON COLUMN public.etl_meta_el.ignore_cols IS '忽略字段' ; -- 字段不会被导入也会被创建
            -- COMMENT ON COLUMN public.etl_meta_el.crt_dt IS '创建日期' ;
    """


def etl_meta_el(conn):
    """
    :param conn:
    :return:
    """
    logger.info('start %s' % func_name)
    # 这里处理的均为人工特殊处理数据 默认dict_type='sp'
    file_nm = data_file_path + '/etl_meta_el.csv'
    import pandas as pd
    df = pd.read_csv(file_nm)
    df = df[~pd.isna(df['tb_nm'])]
    if df.shape[0] > 0:
        df.fillna(
            {"el_type": 'datax', 'trgt_db_cd': default_db_cd, 'sql_where': '', 'sql_pre_sql': 'truncate', 'parallel_num': 4,
             'is_del': 'N', 'ignore_cols': ''}, inplace=True)
        fill_index = df[pd.isna(df['tb_nm_cn'])].index
        if len(df[pd.isna(df['src_tb_nm'])].index) > 0:
            logger.error("src_tb_nm 不能为空")
            raise Exception("src_tb_nm 不能为空")
        if len(df[pd.isna(df['batch_nm'])].index) > 0:
            logger.error("batch_nm 不能为空,必须指定批次名称")
            raise Exception("src_tb_nm 不能为空")
        if len(df[pd.isna(df['src_db_cd'])].index) > 0:
            logger.error("src_db_cd 不能为空")
            raise Exception("src_db_cd 不能为空")
        df.loc[fill_index, 'tb_nm_cn'] = df.loc[fill_index, 'tb_nm']
        df['parallel_num'] = 4  # df['parallel_num'].astype(int)
        conn.df_insert_db(df, tb_nm, pre_sql="delete from " + tb_nm, pk_col='tb_nm')
        df['ignore_cols'] = df['ignore_cols'].apply(lambda x: x.replace(" ", "").replace('，', ','))
        del df['parallel_num']
        os.remove(file_nm)
        df.to_csv(file_nm, index=False)
        logger.info('... end %s' % func_name)


def deal():
    """
    处理入口
    """
    conn = Conn(default_db_cd)
    crt_el_log_tb(conn)
    try:
        # 校验表结构
        if conn.upd_tb_strct(crt_tb_sql=ddl_sql, schm_tb_nm=tb_nm, drop_direct=True):
            etl_meta_el(conn)
    finally:
        conn.close()


if __name__ == '__main__':
    deal()
