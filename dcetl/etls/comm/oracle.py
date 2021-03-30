# cython: language_level=3
def get_table_info_sql(tb_nm, schm_nm):
    """
    获取表结构信息的sql
    :param tb_nm:  表名 没不带schema
    :param schm_nm: schema名称
    :return:
    """
    sql = """
       with pk as(SELECT
                COLUMN_NAME AS column_name 
            FROM
                user_cons_columns cu,
                user_constraints au 
            WHERE
                cu.constraint_name = au.constraint_name and  au.constraint_type='P'
                AND au.OWNER = upper('{schm_nm}')                        
                AND cu.TABLE_NAME = upper('{tb_nm}'))
      select lower(t.table_name) tb_nm,
                   lower(t.column_name) col_nm,
                   lower(case when t.data_type like '%TIMESTAMP%' then 'timestamp' else t.data_type end) col_type,
                   case when t.data_precision>t.data_length then t.data_precision else t.data_length end col_len,
                   t.nullable col_is_null,
                   t.column_id col_sort_id,
                   t.data_scale col_prcn,
                   ucc.comments col_cmnt,
                   tb.comments tb_cmnt,
                   case when pk.column_name is not null then 'Y' else 'N' end is_pk
              FROM dba_tab_columns t
              LEFT JOIN dba_col_comments ucc ON t.column_name = ucc.column_name
              AND t.table_name = ucc.table_name
            left join dba_tab_comments tb on t.table_name=tb.table_name and tb.table_type='TABLE'
            left join pk on  pk.column_name=t.column_name
            WHERE t.table_name =upper('{tb_nm}') and t.owner=upper('{schm_nm}') order by t.column_id 
            """
    sql = sql.format(tb_nm=tb_nm, schm_nm=schm_nm)
    return sql
# print(get_table_info_sql('eemployee','ECOLOGY'))