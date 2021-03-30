# cython: language_level=3


def get_table_info_sql(tb_nm, db_nm):
    """
    获取表结构信息的sql
    :param tb_nm:  表名 没不带schema
    :param db_nm: db_nm
    :return:
    """
    sql = """
               select t.table_name tb_nm,
                      t.column_name col_nm,
                      t.ordinal_position col_sort_id,
                      case when t.is_nullable='YES' then 'Y' else 'N' end col_is_null,
                      t.data_type col_type,
                      coalesce(t.character_maximum_length,t.numeric_precision) col_len,
                     t.numeric_scale col_prcn,
                     t.column_comment col_cmnt,
                     t2.table_comment tb_cmnt,
                     case when column_key='PRI' then 'Y' else 'N' end is_pk
               from information_schema.columns t
               left join information_schema.tables t2 on t.table_name=t2.table_name and t.table_schema=t2.table_schema
               where t.table_name='{tb_nm}' and t.table_schema='{db_nm}' order by t.ordinal_position;"""
    sql = sql.format(tb_nm=tb_nm, schm_nm=db_nm)
    return sql
