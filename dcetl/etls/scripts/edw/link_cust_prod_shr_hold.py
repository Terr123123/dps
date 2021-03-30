import pandas as pd

from dpsetl.comm.batch import BatchParams
from dpsetl.comm.conn import Conn
from dpsetl.comm.datefunc import Date
from dpsetl.comm.job import exec_job
import logging

logger = logging.getLogger()


def recal(start_dt=None, end_dt=None, batch_id=None):
    if batch_id:
        batch_param = BatchParams(batch_id)  # 获取该批次的batch参数文件，返回是一个数据字典类型
    if end_dt is None or start_dt is None:
        td = Date()
        end_dt = end_dt if end_dt else td.add_days(-1)
        start_dt = start_dt if start_dt else td.add_days(-10)
    dt_range = pd.date_range(start=start_dt, end=end_dt, freq='D')
    if not dt_range.empty:
        conn = Conn()
        for dt in dt_range:
            dt = dt.strftime('%Y-%m-%d')
            exec_job(conn, tb_nm='edw.link_cust_prod_shr_hold', job_type='dml', job_params={'busi_dt': dt},
                     batch_id=batch_id, check_tb_strct=False)
            logger.warning("edw.link_cust_prod_shr_hold拉链重算日期%s" % dt)


if __name__ == '__main__':
    recal()
