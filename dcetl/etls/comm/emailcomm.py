# encoding: utf-8
"""
Created on 17-11-8

@author: shaoxw
"""

import smtplib
from etls.comm.loggers import get_logger
from email.mime.text import MIMEText
# from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
# from email.mime.application import MIMEApplication
# from email import encoders
from email.header import Header
from email.utils import parseaddr, formataddr
from etls.comm.wxmsg import send_etl_note
from etls.conf.settings import email_from, email_pwd, email_note_user, email_user, ENV
import json

logger = get_logger()


def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))


def send_email(content, content_type='plain', subject='ERROR-数据平台ETL批量'):
    """
    邮件发送
    :param content:
    :param content_type:
    :param subject:
    :return:
    """
    server = None
    try:
        server = smtplib.SMTP_SSL('mail.chyjr.com', 465)
        server.ehlo()
        server.login(email_user, email_pwd)
        logger.info(server)
        msg = MIMEMultipart()

        msg['From'] = _format_addr(email_from)
        to_list = []
        for i_to in email_note_user:
            i_to = i_to.strip()
            if i_to:
                to_list.append(_format_addr('<%s>' % i_to))
        msg['To'] = ','.join(to_list)
        msg['Subject'] = subject
        msg_text = MIMEText(content, content_type, 'utf-8')
        msg.attach(msg_text)
        server.sendmail(email_user, email_note_user, msg.as_string())
    except Exception as e:
        logger.error(str(e))
    finally:
        try:
            if server:
                server.quit()
        except Exception as e:
            logger.error(str(e))


def send_error_msg(info, tb_nm=None, if_to_wx=True):
    """
     错误消息提醒 注意写好你的提醒函数然后引入调用
    :param if_to_wx: 是否微信消息提醒 默认发出微信通知
    :param info:  消息内容
    :param tb_nm: 表名
    :return:
    """
    if tb_nm:
        info = json.dumps({"表名": tb_nm, "Error": info}, ensure_ascii=False)
    info = str(info)
    if ENV == 'prod':  # 判断是否生产环境
        logger.error("发出错误提醒:\n %s" % info)
        send_email('数据仓库处理异常: ' + info)
        if if_to_wx:  # 是否微信提醒
            send_etl_note('数据仓库处理异常: ' + info)


if __name__ == '__main__':
    send_email('test')
