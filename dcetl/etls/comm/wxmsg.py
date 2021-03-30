# encoding: utf-8
"""
Created on 17-8-3
"toparty": " PartyID1|PartyID2 ",

        "totag": " TagID1 | TagID2 ",
@author: shaoxw
"""
import logging
import requests
from etls.conf.settings import wx_agentId, wx_appSecret, wx_url
from etls.comm.conn import get_conn

logger = logging.getLogger(__name__)


def _get_to_user(note_category):
    """
    获取微信通知人员列表
    :param note_category:
    :return:
    """
    conn = None
    emp_list = []
    try:
        conn = get_conn()
        sql = "SELECT emp_code FROM web.wx_note_user WHERE note_category=%s"
        params = (note_category,)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for rec in cur:
                emp_list.append(rec[0])
    finally:
        if conn:
            conn.close()
    return "|".join(emp_list)


def send_etl_note(msg):
    send(msg, note_category='ETL')


def send(msg, note_category='ETL'):
    """
    发送微信通知
    :param note_category:
    :param msg:
    :return:
    """
    func_name = "发送微信通知: %s, %s" % (note_category, msg)
    logger.info('start %s' % func_name)
    try:
        url = wx_url + "?appSecret=" + wx_appSecret
        touser = _get_to_user(note_category)
        data = {
            "touser": touser,
            "msgtype": "text",
            "agentid": wx_agentId,
            "text": {
                "content": msg
            },
            "safe": 0
        }
        logger.debug(url)
        logger.debug(data)
        r = requests.post(url, json=data, timeout=60)
        if r.status_code == 200:
            res = r.json()
            logger.info(res)
        else:
            logger.info(r.status_code)
    except Exception as e:
        logger.error(e)


def _test():
    """
    msg拼接
    :return:
    """
    msg = "测试: ETL"
    note_category = "ETL"
    send(note_category, msg)


if __name__ == '__main__':
    _test()
