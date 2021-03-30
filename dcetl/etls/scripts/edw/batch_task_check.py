from dpsetl.comm.conn import Conn
import time
from dpsetl.comm.loggers import get_logger, get_handler
from dpsetl.comm.datefunc import get_date
from dpsetl.comm.emailcomm import send_error_msg

tb_nm = 'etl'
logger = get_logger()
logger.addHandler(get_handler(tb_nm))  # 独立日志文件


def check_rs(batch_dt, check_col='is_task_t1_ran', batch_id=None):
    """
    检查结果
    :param batch_dt: 批次日期 yyyy-mm-dd 一般是执行当天日期
    :param check_col: 检查字段
    :param batch_id:  批次ID以后用于检验批次
    :return:
    """
    if check_col == 'is_task_t1_ran':
        sql = "select {check_col} from dw.dim_date where {check_col}='Y' and dt_id='{batch_dt}'"
        sql = sql.format(check_col=check_col, batch_dt=batch_dt)
        conn = Conn()
        rs = conn.query_one(sql)
        conn.close()
        if rs == 'Y':  # 执行完成
            return True
        else:
            return False
    # 下接其他判断条件


def check_and_wait(batch_dt=None, check_col='is_task_t1_ran', batch_id=None, wait_sec=600):
    """
    检查结果
    :param wait_sec: 等待时间，默认是600秒
    :param batch_dt: 批次日期 yyyy-mm-dd 一般是执行当天日期
    :param check_col: 检查字段或者任意可识别的字符或文字
    :param batch_id:  批次ID以后用于检验批次
    :return:
    """
    if batch_dt is None:
        batch_dt = get_date()  # 默认取当前日期
    rs = check_rs(batch_dt, check_col, batch_id)
    i = 0
    waring_time = int(3600 / wait_sec)  # 计算一小时等待次数
    while rs is False:
        i = i + 1
        logger.warning("%s 检查的批次作业还没有执行完成,第%d次等待10分种再继续尝试" % (check_col, i))
        if i % waring_time == 0:  # 每满足一小时就通知
            infos = "%s 检查的批次作业还没有执行完成,等待已经超过%d分钟" % (check_col, int(i * wait_sec / 60))
            send_error_msg(infos, tb_nm=check_col)
            if i * wait_sec >= 10 * 60 * 60:  # 安全机制：超过10小时没人处理直接报错，跳出进程
                raise Exception(infos)
        time.sleep(wait_sec)
        rs = check_rs(batch_dt, check_col, batch_id)
    logger.warning("检测作业通过，开始执行下行作业")
    return True

if __name__ == '__main__' :
    print(check_and_wait('2020-05-20'))
