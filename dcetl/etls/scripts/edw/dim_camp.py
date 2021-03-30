# encoding: utf-8
"""
公司信息维度

Created on 20-06-20

@author: xuzh

times : 每天处理全量
"""

import logging
from dpsetl.comm.dataxy import to_check_tb_strct
from dpsetl.comm.conn import Conn
from dpsetl.settings import BASE_DIR
import jieba

logger = logging.getLogger(__name__)

tb_nm = 'edw.dim_camp'
tb_nm_cn = "活动信息维度"
func_name = tb_nm_cn
proc_all_flag = True
hydict_path = BASE_DIR + '/etl/dpsetl/datafile/hydict.csv'


# 表定义sql


def merge_tag(row):
    if row['x'] == '其他':
        return row['camp_tag']
    elif row['camp_tag'] is None:
        return row['x']
    else:
        tp = (row['x'] + ',' + row['camp_tag']).split(',')
        tp = set(tp)
        return ','.join(tp)


def dim_camp(conn, p_days):
    """
    :param p_days:
    :param conn:
    :return:
    """
    logger.info('start %s' % func_name)
    sql = """
    select 
        'crm_'||id camp_id,
        id src_camp_id,
        'crm' src_sys_cd,
        camp_title camp_nm,
        start_time start_dtm,
        coalesce(m.dic_nm,'其他')  camp_type,
        coalesce(i.launch_org,709) lau_comp_id,
        i.create_time crt_dtm
    from ods.crm_hy_camp_info i 
    left join  dd.src_sys_dic_map m on i.camp_sform=m.dic_key and  group_cd='CAMP_SFORM'   and m.sys_alia_cd='crm'
    where act_status='03' and i.create_time>=current_date-{p_days} ;"""
    df1 = conn.select(sql.format(p_days=p_days))
    mg = {}
    key_map = {
        '产品沙龙': '产品沙龙',
        '项目考察': '项目考察',
        '答谢会': '答谢会',
        '财富下午茶': '下午茶',
        '大型峰会': '峰会',
        '产品路演': '路演',
        '投策会': '投策会',
        '异业合作沙龙活动': '异业合作',
        'V直播下午茶': '直播,下午茶',
        '增值沙龙': '增值沙龙',
        '区域联动产品路演': '路演',
        '视频联动产品路演': '视频,路演',
        '大客户周边休闲游': '大客户,休闲游',
        '新客户公益活动': '新客户,公益',
        '名医养生沙龙': '养生',
        '老客户公益活动': '老客户,公益',
    }
    jieba.load_userdict(hydict_path)
    hydict = [line.rstrip() for line in open(hydict_path)]
    for i in df1.index:
        afterseg = []
        tp = df1.loc[i, 'camp_nm']
        segs = jieba.cut(tp, cut_all=True)
        for seg in segs:
            mg[seg] = mg.get(seg, 0) + 1
            if len(seg) > 1 and seg in hydict:
                afterseg.append(seg)
        df1.loc[i, 'camp_tag'] = ','.join(afterseg)
        if len(afterseg) < 1:
            df1.loc[i, 'camp_tag'] = None
    df1['x'] = df1['camp_type'].apply(lambda x: key_map.get(x.strip(), '其他'))
    df1['camp_tag'] = df1.apply(lambda x: merge_tag(x), axis=1)
    del df1['x']
    df1['camp_tag'] = df1['camp_tag'].fillna('其他')
    pre_sql = "delete from edw.dim_camp where crt_dtm>=current_date-{p_days}".format(p_days=p_days)
    conn.df_insert_db(df1, tb_nm, pre_sql=pre_sql)
    logger.info('... end %s' % func_name)


def deal(p_days=360):
    """
    处理入口
    :param p_days: 增量天数
    :return:
    """
    conn = Conn()
    try:
        with conn:
            to_check_tb_strct(trgt_tb_nm=tb_nm)
            dim_camp(conn, p_days)
    finally:
        conn.close()


if __name__ == '__main__':
    deal()
