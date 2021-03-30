#**ETL架构**
##一、文件目录说明
  1. azkaban
        - 封装了azkaban调度
        - 简化调度配置过程
  
  2. ddl
        + 表结构定义语句
        + 该目录下按schema存储表ddl语句
        + 一个表ddl对应一个文件
  
  3. files
        + 各种文件存储
        + 方便拓展其他任务 例如自定义的datax同步json文件
        + 特殊数据文件
        + flows目录 用于存储 job_flows的工作流任务配置文件，例如T1.csv 
            etl.py T1 flow 执行T1批处理,执行T1工作流
        
  4. etls
  
        4.1. comm目录：
        
            etl通用代码存储路径
            
        4.2. conf
        
            参数配置文件路径
            db.ini 数据库配置文件
            log.yaml 日志配置文件
            params.yaml 自定义参数配置文件
            setting.py 一些主要系统参数和对params.yaml的解析 
            dbmapping 存储每个数据库系统的字段映射，用于异构数据库表结构转化
        
        4.3 scripts:
        
            4.3.1 任务脚本存储目录，目前支持sql和Python脚本 
            4.3.2 一个任务脚本对应一张表处理
            4.3.3 按schema分目录存储，例如edw目录对应schema edw
            4.3.4 特殊脚本存在sql或tools目录下
         
        4.4 etl.py
        
            主程序入口
            命令样式 etl.py tb_nm job_type 后面跟着 一一对应的kv参数(个数不限)
                tb_nm  表名: 例如 dw.dim_cust(可以是多个表名用“,”分割，不能有空格)
                job_type job类型:
                    ddl/dml/batch_el/func/el/sql/flow, 
                    func 是指对应python文件中的函数名称 
                    el指同步表,batch_el批次同步
                参数格式 
                   参数名称需要以-或者--开头然后紧跟着参数值,例如-start_dt 2019-10-01 或者 --start_dt 2019-10-01
            样例
            etl.py  task_infos deal -sys_dt 2019-12-10 
                    执行目录下task_infos.py python文件中的deal函数 sys_dt是deal函数的参数
            etl.py  edw.dim_cust dml -batch_dt 2019-12-10 
                    执行的edw/目录下的 dim_cust.sql  batch_dt 是传入参数
            etl.py  edw.fact_cust_leads dml -schm rtp -p_days 10
                    执行的edw/目录下的 fact_cust_leads.sql  p_days 是传入参数
            etl.py hq_hr.estatus el -conn ibm -src_tb_nm ecology.estatus -src_db_cd hr  -c False
                    临时导入一张表 ：从hr(db.ini中配置的数据库标识)中的 ecology.estatus表
                        导入目标库 -conn ibm(不设置则默认default_db_cd)中的hq_hr.estatus表
                    默认会自动建表并根据表结构的变化而变化
                     -c False 表示不校验表结构
            etl.py T1 batch_el -batch_id 202008200101 与 etl.py batch_el el -batch_id 202008200101 -batch_nm T1 等效
            etl.py T1 flow 执行T1批处理
            
             