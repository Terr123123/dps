from dpsetl.comm.conn import Conn


def gen_sql(where_keys):
    """
    生成执行sql
    :param where_keys:
    :return:
    """
    sql = " where trans_dt between '{start_trans_dt}' and '{end_trans_dt}'".format(**where_keys)
    sql_list = [sql]
    cols = ['emp_cd', 'prod_id', 'accnt_cd', 'comp_id',
            'cont_cd', 'prod_shr_type_id', 'cust_no', 'trans_cd', 'src_sys_cd']
    i = 1
    params = []
    for c in cols:
        vals = where_keys.get(c, None)
        if vals and len(vals.strip()) > 0:
            sql_list.append("and")
            sql_list.extend([c, "= %s"])
            params.append(vals)
            i += 1
    where_sql = " ".join(sql_list)
    select_sql = """
    insert into stg.stg_mop_adj_map(order_cd,rule_id,adj_std_kpi,adj_std_amnt_by_ratio,adj_std_amnt,rule_add_dt) 
    select 
                    order_cd,
                    '{rule_id}' rule_id,
                    {adj_std_kpi} adj_std_kpi,
                    {adj_std_amnt_by_ratio} adj_std_amnt_by_ratio,
                    {adj_std_amnt} adj_std_amnt,
                    '{rule_add_dt}' rule_add_dt
                 from edw.fact_trans """.format(**where_keys)
    select_sql = select_sql + where_sql + " and is_cncl='N'"  # 没有被取消的单据
    return select_sql, params


def deal(p_days=180):
    sql = "select * from stg.spc_mop_rule where rule_add_dt>=current_date-{0}".format(p_days)
    conn = Conn()
    df = conn.select(sql)
    # df.set_index('rule_id', drop=False,inplace=True)
    df['rule_add_dt'] = df['rule_add_dt'].astype(str)
    df['start_trans_dt'] = df['start_trans_dt'].astype(str)
    df['end_trans_dt'] = df['end_trans_dt'].astype(str)
    df.fillna({'start_trans_dt': '2000-01-01', 'end_trans_dt': '9999-12-31'}, inplace=True)
    rs = df.to_dict(orient='records')  # df.to_json(orient='records',date_format='iso')
    if rs:
        conn.exec("delete from stg.stg_mop_adj_map where rule_add_dt>=current_date-{0}".format(p_days))
        for r in rs:
            insert_sql, params = gen_sql(r)
            conn.exec(insert_sql, params, if_print_sql=True)  # 不打印sql日志
    # todo 后续规则是否匹配或应用做个校验


if __name__ == '__main__':
    deal()
