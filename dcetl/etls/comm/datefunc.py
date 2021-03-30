"""
create time 2019-12-30
create by xuzh
https://www.cnblogs.com/haoshine/p/5329774.html 参考
"""

from time import strftime, localtime, strptime, mktime
from datetime import timedelta, datetime
import calendar
import re
from etls.comm.conn import Conn
import logging

logger = logging.getLogger()


def get_days_of_mon(year_mon):
    """''
    返回指定月份的天数即最后一天日期
    :param year_mon: yyyy-mm 或者yyyymm 或者 yyyy-mm-dd
    """
    year_mon = str(year_mon).replace("-", "")[0:6]
    if year_mon.isdigit() and len(year_mon) == 6:
        year, mon = divmod(int(year_mon), 100)
        return calendar.monthrange(year, mon)[1]
    else:
        raise Exception("%s日期格式不规范" % year_mon)


def get_date(some_date=None, diff_days=0, pattern='%Y-%m-%d'):
    """
    获取当前日期或某个日期加减天数后日期
    time_struct与datetime之间的转换可以通过中间状态string来完成
    或者
      dt.timetuple() 可以使得datetime转struct_time
      datetime.fromtimestamp(mktime(struct)) 可以使得struct_time转sdatetime
      datetime.datetime.strptime(date, '%Y-%m-%d')  strptime
    :param some_date: 可以指定后天作为基数，然后计算。不指定则取当天
    :param pattern: 指定返回格式，也是some_date的输入格式。如果有的话
    :param diff_days 表示差的天数，可以是为负，0表示是当前日期 表示加减天数
    :return 返回字符日期
    """
    if some_date:
        today = datetime.strptime(some_date, pattern)  # 字符串转日期 datetime.datetime
    else:
        today = datetime.now()  # 当前时间
    diff_dt = today + timedelta(days=diff_days)
    return diff_dt.strftime(pattern)  # 字符串型 datetime.datetime 转字符串 str from time


def get_date_ts(some_date=None, diff_days=0, pattern='%Y-%m-%d'):
    """
    获取当前日期或某个日期加减天数后日期 时间戳
    time_struct与datetime之间的转换可以通过中间状态string来完成
    :param some_date: 可以指定后天作为基数，然后计算。不指定则取当天
    :param pattern: 指定返回格式，也是some_date的输入格式。如果有的话
    :param diff_days 表示差的天数，可以是为负，0表示是当前日期 表示加减天数
    :return 返回时间戳
    """
    diff_dt = get_date(some_date, diff_days, pattern)
    return int(dtm_to_ts(diff_dt, pattern))


def ts_to_dtm(timenum, dt_format='%Y-%m-%d %H:%M:%S'):  # 整数型时间戳转化为日期字符串
    """
    时间戳转日期字符型
    :param timenum:
    :param dt_format:
    :return:
    """
    time_stamp = timestamp_to_timestamp10(timenum)  # 毫秒微妙处理
    # time_array = time.localtime(time_stamp)    time.strftime("%Y-%m-%d %H:%M:%S.%s", time_array)
    return datetime.fromtimestamp(time_stamp).strftime(dt_format)


def dtm_to_ts(dt, dt_format="%Y-%m-%d %H:%M:%S"):
    """
    输入的日期字符型转时间戳
    :param dt:
    :param dt_format:
    :return:
    """
    return mktime(strptime(dt, dt_format))


def get_rpt_dtm(batch_dt, dtm_type):
    """
    返回清算的时间 仅对海银清算时间
    :param batch_dt:日期
    :param dtm_type:
        -- 取上次报表结束时间  cvrt_typ: last_rpt_end_dtm
        -- 取今天报表结束时间  cvrt_typ: tdy_rpt_end_dtm
        -- 取下一个工作日     cvrt_typ:  nxt_workday
        -- 取季度的第一天     cvrt_typ： quar_start
        -- 取下个季度的第一天 cvrt_typ： quar_end
    :return:
    """
    if dtm_type in ['last_rpt_end_dtm', 'tdy_rpt_end_dtm', 'nxt_workday', 'quar_start', 'quar_end']:
        sql = """
           SELECT
               dw.cvrt_dt('{batch_dt}', '{dtm_type}') AS dt  -- 上个工作日报表结束时间
           """.format(batch_dt=batch_dt, dtm_type=dtm_type)
        dps = Conn("DPS")
        return dps.query_one(sql)
    else:
        return None


def get_from_dim_date(batch_dt, col_nm):
    """
    返回清算的时间 仅对海银清算时间
    :param batch_dt:日期
    :param col_nm: 指定的字段
    :return:
    """
    sql = """
       SELECT {col_nm} from dw.dim_date where dt_id='{batch_dt}'
        """.format(batch_dt=batch_dt, col_nm=col_nm)
    dps = Conn("DPS")
    try:
        rs = dps.query_one(sql)
        return rs
    except Exception as e:
        str(e)
        return None


def timestamp_to_timestamp10(time_stamp):
    """
    int或float
    将毫秒或者微妙int转化为10位的秒级别
    :param time_stamp:
    :return:
    """
    time_stamp_str = str(time_stamp)
    index_of_dot = time_stamp_str.find(".")
    if 0 <= index_of_dot <= 10:  # 是否带有.的小数位
        return time_stamp
    if index_of_dot > 10:  # 微妙或者毫秒带有小数位 剔除小数部分
        time_stamp_str = time_stamp_str[0:index_of_dot]
    time_stamp = time_stamp * (10 ** (10 - len(time_stamp_str)))
    return time_stamp


def diff_between_date(dtm1, dtm2, rs_type='days'):
    """
    比较两个日期的时间，返回days或mons或者seconds
    :param dtm1:
    :param dtm2:
    :param rs_type:  可以选择days,mon,secondes,即返回对应的差值
    :return:
    """
    dt1 = Date(dtm1)
    dt2 = Date(dtm2)
    return dt1.compare(dt2, rs_type)


def cast_strs_to_time(date_tm):
    """
    将字符串尽可能的转换成日期型，无法转换的NONE
    :param date_tm:
    :return:
    """
    date_tm = str(date_tm).strip()
    if date_tm and len(date_tm) >= 6:
        date_tm = date_tm.replace("-", "").replace(" ", "").replace(":", "")
        try:
            dt = strptime(date_tm[0:14].ljust(14, '0'), '%Y%m%d%H%M%S')
            return dt
        except Exception as e:
            logger.debug("%s 尝试按8位日期转换 %s" % (date_tm, str(e)))
            try:
                tp = date_tm[0:8]
                if len(tp) == 6:  # 如果是6位数，则需要全日期，取当前的日期，如果当天日大于指定月份的最大天数则取最大天数
                    today = localtime().tm_mday
                    max_days = get_days_of_mon(tp)
                    if today > max_days:
                        today = max_days
                    tp = tp + str(today).zfill(2)
                dt = strptime(tp, '%Y%m%d')
                return dt
            except Exception as e:
                logger.debug("%s无效时间格式%s" % (date_tm, str(e)))
                return None
    else:
        # 无效 日期格式
        logger.debug(date_tm + "不是有效日期格式")
        return None


class Date(object):
    def __init__(self, dt_or_ts=None):  # 字符串格式yyyy-mm-dd或者 yyyy-mm-dd hh:mm:ss 或者秒数 yyyy
        """
        初始化
        :param dt_or_ts:
        1、可以是yyyy-mm-dd yyyy-mm-dd hh:mm:ss 的字符串
        2、yyyymm 格式传入的必须是字符串，不能是整数（整数型会被认为是时间戳）
        3、时间戳的是整数型
        """
        if dt_or_ts:
            if isinstance(dt_or_ts, str):
                tp = cast_strs_to_time(dt_or_ts)  # 尽可能的转化为日期
                if tp and tp.tm_year > 1900:
                    # nowtime 为 struct_time
                    self.nowtime = tp  # 如果转化成功
                else:
                    self.nowtime = localtime()  # 不成功取当前日期
            elif isinstance(dt_or_ts, (int, float)):  # 是int 或者float
                date_time = timestamp_to_timestamp10(dt_or_ts)  # 转化为秒级别
                self.nowtime = localtime(date_time)
        # struct_time(tm_year=, tm_mon=, tm_mday=, tm_hour=0, tm_min=0, tm_sec=0, tm_wday=0, tm_yday=, tm_isdst=0)
            else:
                raise Exception("输入的初始化值必须是日期型字符串或者秒级别的时间戳或者不传入值即None")
        else:  # 不传入参数，直接取当前日期
            self.nowtime = localtime()  # 不成功取当前日期
        try:
            self.year = strftime("%Y", self.nowtime)
            self.mon = strftime("%m", self.nowtime)
            self.day = strftime("%d", self.nowtime)
            self.hour = strftime("%H", self.nowtime)
            self.min = strftime("%M", self.nowtime)
            self.sec = strftime("%S", self.nowtime)
            self.__dt = datetime.fromtimestamp(mktime(self.nowtime)).date()  # struct_time 转日期型datetime.datetime  内部变量
            self.dt = strftime("%Y-%m-%d", self.nowtime)  # 字符串型
            self.dtm = strftime("%Y-%m-%d %H:%M:%S", self.nowtime)  # '%Y-%m-%d %H:%M:%S.%f
            self.ts = mktime(self.nowtime)
        except Exception as e:
            raise Exception(str(e) + " for %s (输入日期参数不合规)" % dt_or_ts)

    def yyyymmdd(self):
        """
        返回 format="YYYYMMDD"
        """
        return self.year + self.mon + self.day  # 字符串型

    def batch_dt(self):
        """
        返回 format="YYYY-MM-DD"
        """
        return strftime("%Y-%m-%d", self.nowtime)  # 字符串型

    def format(self, pattren="%Y-%m-%d %H:%M:%S"):
        """
        按指定格式输出
        :param pattren:
        :return:
        """
        return strftime(pattren, self.nowtime)  # 字符串型

    def add_days(self, n=0):
        """
        加减天数获取日期
        date format = "YYYY-MM-DD"
        """
        if n < 0:
            n = abs(n)
            return (self.__dt - timedelta(days=n)).strftime('%Y-%m-%d')
        else:
            return (self.__dt + timedelta(days=n)).strftime('%Y-%m-%d')

    def add_mons(self, diff_months=-1, return_ym=False, return_last_day=False):
        """
              根据日期加减月数
              :param  diff_months： 月份差数，可以是正数也可以是负数 默认是 -1
              :param  return_ym： 是否返回返年月yyyy-mm，默认返回yyyy-mm-dd
              :param return_last_day 是返回月份的最后一天
              :return:
        """
        date_format = "%d-%02d-%02d"  # 不足2位补0
        year = int(self.year)
        mon = int(self.mon)
        day = int(self.day)
        if diff_months >= 0:  # 判断加减
            add_years = int(diff_months / 12)
            add_month = diff_months % 12
        else:
            add_years = 0 - int(abs(diff_months) / 12)
            add_month = 0 - abs(diff_months) % 12
        year = year + add_years
        mon = mon + add_month
        if mon > 12:  # 判断是否跨年
            year += 1
            mon = mon % 12
        elif mon <= 0:
            year -= 1
            mon += 12
        if return_ym:  # 是否只返回年月
            rs = str(year) + '-' + str(mon).zfill(2)
        else:
            last_day = calendar.monthrange(int(year), int(mon))[1]
            if day > last_day:
                day = last_day
            if return_last_day:
                day = last_day
            rs = date_format % (int(year), int(mon), day)
        return rs

    def this_mon(self):  # 本月月份 YYYY-MM
        return self.year + '-' + self.mon.zfill(2)

    def last_dt_this_mon(self):  # 本月月份本月最后一天 yyyy-mm-dd
        days = calendar.monthrange(int(self.year), int(self.mon))[1]  # 本月最大天数
        return self.year + '-' + self.mon.zfill(2) + '-' + str(days).zfill(2)

    def days_of_this_mon(self):  # 本月月份天数
        days = calendar.monthrange(int(self.year), int(self.mon))[1]
        return days

    def fst_dt_this_mon(self):  # 本月月份第一天 yyyy-mm-dd
        return self.year + '-' + self.mon.zfill(2) + '-01'

    def last_mon(self):  # 上月月份 YYYY-MM
        year = int(self.year)
        mon = int(self.mon)
        if mon == 1:
            mon = 12
            year -= 1
        else:
            mon -= 1
        return str(year) + '-' + str(mon).zfill(2)

    def last_dt_last_mon(self):  # 上月月份最后一天 YYYY-MM-DD
        year = int(self.year)
        mon = int(self.mon)
        if mon == 1:
            mon = 12
            year -= 1
        else:
            mon -= 1
        days = calendar.monthrange(int(year), int(mon))[1]
        return self.last_mon() + '-' + str(days).zfill(2)

    def fst_dt_last_mon(self):  # 上月月份第一天 YYYY-MM-DD
        return self.last_mon() + '-01'

    def yday(self):
        """昨日日期"""
        return self.add_days(-1)

    def tmw(self):
        """tomorrow 明天日期"""
        return self.add_days(1)

    def last_rpt_end_dtm(self):
        """
        获取对应的报表清算结束时间
        :return:
        """
        return get_rpt_dtm(self.dt, 'last_rpt_end_dtm')

    def tdy_rpt_end_dtm(self):
        """
        取今天报表结束时间
        :return:
        """
        return get_rpt_dtm(self.dt, 'tdy_rpt_end_dtm')

    def next_workday(self):
        """
        获取下个工作日
        :return:
        """
        return get_rpt_dtm(self.dt, 'nxt_workday')

    def compare(self, dt_or_ts, rs_type="sec"):
        """
        比较两个类的大小
        :param rs_type: second或者days 返回类型 默认返回相差的秒数，days表示返回相差天数
        :param dt_or_ts:
        :return: 返回的self 减去dt_or_ts的秒数 0 表示相等，正数表示self大于dt_or_ts
        """
        if isinstance(dt_or_ts, Date):  # 如果如果传入的Date类
            dt2 = dt_or_ts
        else:
            dt2 = Date(dt_or_ts)
        if rs_type.startswith("day"):
            return (self.__dt - dt2.__dt).days
        elif rs_type.startswith("mon"):
            return (int(self.year) - int(dt2.year)) * 12 + (int(self.mon) - int(dt2.mon))
        return int(self.ts - dt2.ts)

    def get_from_dim_date(self, param):
        """
        从dim_date表获取参数
        :param param:
        :return:
        """
        rs = get_from_dim_date(self.dt, param)
        if rs:
            return rs
        else:
            return get_rpt_dtm(self.dt, param)

    def get_param(self, key):
        """
        用与sql文件中系统参数获取，sql文件系统参数必须是sys_开头
        :param key:key已经是被剔除了sys_的变量
        :return:返回对应参数值
        """
        try:
            all_keys = self.__dir__()
            key = key.lower()
            all_var = self.__dict__
            if key in all_var:
                return all_var[key]
            if key in all_keys:
                return eval("self." + key + "()")
            rs = self.get_from_dim_date(key)
            if rs:
                return rs
            match = re.compile(r'[+-]\d+day|[+-]\d+mon')
            lst = match.findall(key)
            if len(lst) > 0:
                tp = lst[0]
                if tp.endswith('mon'):
                    tp = int(tp[0:-3])
                    return self.add_mons(tp)
                elif tp.endswith('day'):
                    tp = int(tp[0:-3])
                    return self.add_days(tp)
                else:
                    return key + ' 无效参数'
            else:
                return key + ' 无效参数'
        except Exception as e:
            logger.info(str(e))
            return key + ' 无效参数'


def test(strs="2019-12-10 22:23:45"):
    ts1 = dtm_to_ts(strs)
    d1 = Date()
    d2 = Date('2017-12-10 23:34:34')
    d11 = Date(ts1)

    logger.info(d1.format('%Y-%m-%d %H:%M:%S'))
    logger.info(d1.mon)
    logger.info(mktime(d1.nowtime))
    logger.info(d1.compare(d2, 'mon'))
    logger.info(d1.batch_dt())
    logger.info(d1.nowtime)
    logger.info(d1.get_param("last_-3days"))  # 获取减少去三天后的日期
    logger.info(d1.add_mons(20))
    logger.info(d11.add_mons(-1))
    logger.info(d1.last_mon())
    logger.info(d1.last_dt_last_mon())
    d1 = Date('2020-04-08')
    logger.info(d1.format('%Y-%m-%d %H:%M:%S'))
    logger.info(mktime(d1.nowtime))
    logger.info(d1.mon)
    logger.info(mktime(d1.nowtime))
    logger.info(d1.compare(d1, 'mon'))
    logger.info(d1.batch_dt())
    logger.info(d1.nowtime)
    logger.info(d1.get_param("last_-3days"))  # 获取减少去三天后的日期
    logger.info(d1.add_mons(20))
    logger.info(d1.add_mons(-1))
    logger.info(d1.last_mon())
    logger.info(d1.last_dt_last_mon())
    print("从dim_date获取参数", d1.get_param("last_work_day_dt"))


if __name__ == "__main__":
    test()
