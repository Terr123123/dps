from dpsetl.comm.conn import Conn
from dpsetl.comm.table import Table
import logging

logger = logging.getLogger()


def find_all_index(arr, item):
    return [i for i, a in enumerate(arr) if a == item]


def deal():
    sql = """
            SELECT
                o.id org_id,
                o.org_code          AS src_org_cd,
                o.org_name     AS org_nm,
                case when o.status='normal' then 'Y' else 'N' end is_valid,
                o.manager_emp_no    AS org_mgr_cd,
                o.parent_id         AS parent_org_id,
                o1.org_name  parent_org_nm,
                o1.manager_emp_no   AS parent_org_mgr_cd,
                o.org_type,
                case when o.org_type ='company' then 'Y' else 'N' end is_comp,
                case when  o.id=709 then '.709.709.709.709.' else o.org_seq end org_seq,
                case when o.id=709 then 0 
                     when o.org_type='area' then 1 
                     when o.org_type='city' then 2  else 3 end org_lvl
            FROM portal.hyb_organization o
            LEFT JOIN portal.hyb_organization o1 ON o.parent_id=o1.id  
            WHERE o.org_type in('company','area','city') AND o.id NOT IN(710,711)
    """
    conn = Conn()
    logger.info("开始处理表stg.stg_dim_org")
    df = conn.select(sql)
    df.set_index("org_id", inplace=True, drop=False)
    lvl_dict = df.to_dict()['org_lvl']  # 每个org的等级映射表

    def get_lvl_by_seq(row, max_lvl=4):  # 计算出各个等级的org_id
        org_seq = row['org_seq']
        comp_list = org_seq[1:-1].split(".")
        lvl_list = list(map(lambda x: lvl_dict.get(int(x), -1), comp_list))
        if -1 in lvl_list:  # 不存在的org_id 删除掉
            idx = find_all_index(lvl_list, -1)
            for d in idx:
                del lvl_list[d]
                del comp_list[d]
        if len(comp_list) < 4:  # 不足4级的（总部-区域-城市-分公司）进行计算
            rs = []
            # 初始化  print(comp_list, lvl_list)
            last_lvl_idx = 0  # 上个级别下标
            last_comp_id = 709  # 上个级别的ID
            for k in range(max_lvl):
                if k == 0:
                    last_lvl_idx = 0
                    last_comp_id = comp_list[0]
                    rs.append(last_comp_id)
                else:
                    if last_lvl_idx >= len(comp_list) - 1:
                        last_lvl_idx = len(comp_list) - 2
                    i = lvl_list[last_lvl_idx + 1]
                    # print(i, k, last_lvl_idx, rs)
                    if k < i:
                        rs.append(last_comp_id)
                    else:
                        last_comp_id = comp_list[last_lvl_idx + 1]
                        last_lvl_idx = last_lvl_idx + 1
                        rs.append(last_comp_id)
            # print(rs)
            return ','.join(rs), str(lvl_list)
        return ','.join(comp_list), str(lvl_list)

    df[['comp_list', 'lvl_list']] = df.apply(get_lvl_by_seq, axis=1, result_type="expand")
    # （总部-区域-城市-分公司）分别对应lvl0', 'lvl1', 'lvl2', 'lvl3' 存的的是org_id
    df[['lvl0', 'lvl1', 'lvl2', 'lvl3']] = df['comp_list'].str.split(",", expand=True)
    tb = Table(conn, 'stg.stg_dim_org')  # 会校验表结构
    df.reset_index(inplace=True, drop=True)
    tb.insert(df, pre_sql='truncate table stg.stg_dim_org')


if __name__ == '__main__':
    deal()
