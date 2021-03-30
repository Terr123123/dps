# encoding: utf-8
"""
@Title: 产品分类维度

@Creator: ChenYuanbin
@CreateTime: 19-11-8
"""

import logging

from etls.comm.conn import Conn

logger = logging.getLogger(__name__)
tb_nm = 'edw.dim_prod_cat'
tb_nm_cn = "产品分类"
func_name = tb_nm_cn
proc_all_flag = False  # 全量处理
# 表定义sql
ddl_sql = """
    CREATE TABLE edw.dim_prod_cat (
        cat_id varchar(20) NOT NULL, -- 分类ID
        cat_nm varchar(10) NOT NULL, -- 分类名称
        cat_lvl int2 NULL, -- 分类级别 0:产品大类 1:一级分类 2:二级分类 3:三级分类
        prnt_cat_id varchar(20) NULL, -- 上级分类ID
        cat_sort int2 NULL, -- 分类排序(同级)
        is_show varchar(1) NOT NULL, -- 是否展示
        CONSTRAINT dim_prod_cat_pk PRIMARY KEY (cat_id)
    )
    WITH (
        OIDS=FALSE
    ) ;
    
    COMMENT ON TABLE edw.dim_prod_cat IS '产品分类维度表';
    COMMENT ON COLUMN edw.dim_prod_cat.cat_id IS '分类ID' ;
    COMMENT ON COLUMN edw.dim_prod_cat.cat_nm IS '分类名称' ;
    COMMENT ON COLUMN edw.dim_prod_cat.cat_lvl IS '分类级别' ;
    COMMENT ON COLUMN edw.dim_prod_cat.prnt_cat_id IS '上级分类ID' ;
    COMMENT ON COLUMN edw.dim_prod_cat.cat_sort IS '分类排序' ;
    COMMENT ON COLUMN edw.dim_prod_cat.is_show IS '是否展示' ;
"""


def dim_prod_cat(conn):
    """
    :param conn:
    :return:
    """
    logger.info('start %s' % func_name)
    sql = """
    TRUNCATE TABLE edw.dim_prod_cat;
    INSERT INTO edw.dim_prod_cat(
        cat_id,     -- 分类ID
        cat_nm,     -- 分类名称
        cat_lvl,    -- 分类级别
        prnt_cat_id, -- 上级分类ID
        cat_sort,   -- 分类排序
        is_show     -- 是否展示
    ) VALUES
    -- 私募产品
     ('pf', '私募产品', 0, NULL, 1, 'Y')
    ,('pf_1', '类固收', 1, 'pf', 1, 'Y')
    ,('pf_2', '股权类', 1, 'pf', 2, 'Y')
    ,('pf_3', '证券类', 1, 'pf', 3, 'Y')
    ,('pf_4', '私募其他类', 1, 'pf', 4, 'Y')
    ,('pf_5', '现金管理类', 1, 'pf', 5, 'Y')
    ,('pf_6', '信托产品', 1, 'pf', 6, 'Y')
    ,('pf_1_11', '地产产品', 2, 'pf_1', 1, 'Y')
    ,('pf_1_12', '融资租赁产品', 2, 'pf_1', 2, 'Y')
    ,('pf_1_13', '保理产品', 2, 'pf_1', 3, 'Y')
    ,('pf_1_14', '消费金融产品', 2, 'pf_1', 4, 'Y')
    ,('pf_1_15', '主动管理型产品', 2, 'pf_1', 5, 'Y')
    ,('pf_1_16', '其他类固收产品', 2, 'pf_1', 6, 'Y')
    ,('pf_2_21', 'VC/PE产品', 2, 'pf_2', 7, 'Y')
    ,('pf_2_22', '地产股权产品', 2, 'pf_2', 8, 'Y')
    ,('pf_2_23', '并购产品', 2, 'pf_2', 9, 'Y')
    ,('pf_2_24', '股权FOF产品', 2, 'pf_2', 10, 'Y')
    ,('pf_2_25', '新三板产品', 2, 'pf_2', 11, 'Y')
    ,('pf_3_31', '二级市场产品', 2, 'pf_3', 12, 'Y')
    ,('pf_3_32', '定向增发产品', 2, 'pf_3', 13, 'Y')
    ,('pf_3_33', '证券FOF产品', 2, 'pf_3', 14, 'Y')
    ,('pf_3_34', '量化对冲产品', 2, 'pf_3', 15, 'Y')
    ,('pf_4_41', '影视产品', 2, 'pf_4', 16, 'Y')
    ,('pf_4_42', '其他创新型产品', 2, 'pf_4', 17, 'Y')
    ,('pf_5_51', '日日盈', 2, 'pf_5', 18, 'Y')
    ,('pf_5_52', '月月盈', 2, 'pf_5', 19, 'Y')
    ,('pf_6_61', '政信类', 2, 'pf_6', 20, 'Y')
    ,('pf_6_62', '房地产类', 2, 'pf_6', 21, 'Y')
    ,('pf_6_63', '工商企业类', 2, 'pf_6', 22, 'Y')
    ,('pf_6_64', '消费金融类', 2, 'pf_6', 23, 'Y')
    ,('pf_6_65', '其他类', 2, 'pf_6', 24, 'Y')
    ,('pf_1_13_131', '供应链保理类', 3, 'pf_1_13', 1, 'Y')
    ,('pf_1_13_132', '地产保理类', 3, 'pf_1_13', 2, 'Y')
    ,('pf_1_16_161', '股权夹层产品', 3, 'pf_1_16', 3, 'Y')
    ,('pf_1_16_162', '股票质押产品', 3, 'pf_1_16', 4, 'Y')
    -- 公募产品
    ,('hyf', '公募产品', 0, NULL, 2, 'Y')
    ,('hyf_y', '公募基金', 1, 'hyf', 7, 'Y')
    ,('hyf_y_20', '指数型', 2, 'hyf_y', 101, 'Y')
    ,('hyf_y_21', '股票型', 2, 'hyf_y', 102, 'Y')
    ,('hyf_y_22', '混合型', 2, 'hyf_y', 103, 'Y')
    ,('hyf_y_23', '配置型', 2, 'hyf_y', 104, 'Y')
    ,('hyf_y_24', '债券型', 2, 'hyf_y', 105, 'Y')
    ,('hyf_y_25', '货币型', 2, 'hyf_y', 106, 'Y')
    ,('hyf_y_26', '海外基金', 2, 'hyf_y', 107, 'Y')
    ,('hyf_y_27', '短期理财型', 2, 'hyf_y', 108, 'Y')
    ,('hyf_y_28', '保本型', 2, 'hyf_y', 109, 'Y')
    ,('hyf_y_29', '其他类型', 2, 'hyf_y', 110, 'Y')
    ,('hyf_y_2a', '公募专户', 2, 'hyf_y', 111, 'Y')
    ,('hyf_y_2b', 'FOF', 2, 'hyf_y', 112, 'Y')
    -- 公募产品(代销的私募产品)
    ,('hyf_y_30', '指数型', 2, 'hyf_y', 113, 'N')
    ,('hyf_y_31', '股票型', 2, 'hyf_y', 114, 'N')
    ,('hyf_y_32', '混合型', 2, 'hyf_y', 115, 'N')
    ,('hyf_y_33', '配置型', 2, 'hyf_y', 116, 'N')
    ,('hyf_y_34', '债券型', 2, 'hyf_y', 117, 'N')
    ,('hyf_y_35', '货币型', 2, 'hyf_y', 118, 'N')
    ,('hyf_y_36', '海外基金', 2, 'hyf_y', 119, 'N')
    ,('hyf_y_37', '短期理财型', 2, 'hyf_y', 120, 'N')
    -- 多元产品
    ,('other', '多元产品', 0, NULL, 3, 'Y')
    ,('other_i', 'I类', 1, 'other', 8, 'Y')
    ,('other_f', 'F类', 1, 'other', 9, 'Y')
    ,('other_ft', '家族信托', 1, 'other', 10, 'Y')
    ,('other_yqd', 'YQD', 1, 'other', 11, 'Y')
    ,('other_ph', '普惠e融', 1, 'other', 12, 'Y')
    ,('other_cft', '财富通', 1, 'other', 13, 'Y')
    ,('other_hyh', 'HYH', 1, 'other', 14, 'Y')
    ,('other_i_1', 'I1', 2, 'other_i', 201, 'Y') -- 境外保险
    ,('other_i_2', 'I2', 2, 'other_i', 202, 'Y') -- 境内保险
    ,('other_f_1', 'F1', 2, 'other_f', 203, 'Y') -- 海外金融
    ,('other_f_2', 'F2', 2, 'other_f', 204, 'Y') -- 海外置业
    ,('other_f_3', 'F3', 2, 'other_f', 205, 'Y') -- 海外移民
    ;
    """
    conn.exec(sql)

    logger.info('... end %s' % func_name)


def deal():
    """
    处理入口
    :return:
    """
    conn = Conn('ibm')
    try:
        with conn:
            # 表结构校验
            if conn.upd_tb_strct(crt_tb_sql=ddl_sql, schm_tb_nm=tb_nm, drop_direct=proc_all_flag):
                # 产品分类
                dim_prod_cat(conn)
    finally:
        conn.close()


if __name__ == '__main__':
    deal()
