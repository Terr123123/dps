# cython: language_level=3
def get_table_info_sql(tb_nm, db_nm):
    """
    获取表结构信息的sql
    :param tb_nm:  表名 没不带schema
    :param db_nm: db_nm
    :return:
    """
    sql = """SELECT
                       sysobjects.name AS tb_nm ,
                       sysproperties.[value] AS tb_cmnt ,
                       syscolumns.name AS col_nm ,
                       properties.[value] AS col_cmnt ,
                       systypes.name AS col_type ,
                       syscolumns.length AS col_len ,
                       ISNULL(COLUMNPROPERTY(syscolumns.id, syscolumns.name,'Scale'), 0) AS col_prcn ,
                       CASE WHEN syscolumns.isnullable=0 THEN 'N' ELSE 'Y' END AS col_is_null,
                       syscolumns.id col_sort_id,
                       'N' as is_pk
                   FROM syscolumns                            -- 数据表字段
                   INNER JOIN sysobjects                        -- 数据对象
                     ON sysobjects.id = syscolumns.id
                   INNER JOIN systypes                         -- 数据类型
                     ON syscolumns.xtype = systypes.xtype
                   LEFT OUTER JOIN sys.extended_properties properties       -- 字段属性信息
                     ON syscolumns.id = properties.major_id
                    AND syscolumns.colid = properties.minor_id
                   LEFT OUTER JOIN sys.extended_properties sysproperties                -- 表属性信息
                     ON sysobjects.id = sysproperties.major_id
                    AND sysproperties.minor_id = 0
                   LEFT OUTER JOIN syscomments                -- 注释信息
                     ON syscolumns.cdefault = syscomments.id
                   LEFT OUTER JOIN sysindexkeys                -- 索引中的键或列的信息
                     ON sysindexkeys.id = syscolumns.id
                    AND sysindexkeys.colid = syscolumns.colid
                   LEFT OUTER JOIN sysindexes                  -- 数据库 索引表
                     ON sysindexes.id = sysindexkeys.id
                    AND sysindexes.indid = sysindexkeys.indid
                   LEFT OUTER JOIN sysforeignkeys
                     ON sysforeignkeys.fkeyid = syscolumns.id
                    AND sysforeignkeys.fkey = syscolumns.colid
                   left join INFORMATION_SCHEMA.tables ts on sysobjects.name =ts.table_name
                   WHERE (sysobjects.xtype = 'U') and ts.Table_catalog='{db_nm}' and sysobjects.name='{tb_nm}'
                   order by syscolumns.id"""
    sql = sql.format(tb_nm=tb_nm, db_nm=db_nm)
    return sql
# print(get_table_info_sql('eemployee','ECOLOGY'))
