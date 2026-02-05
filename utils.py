import logging
import time
import functools
import smtplib
import os
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.header import Header

# [修复] 强制北京时间 (UTC+8)
def get_beijing_time():
    utc_now = datetime.now(timezone.utc)
    beijing_time = utc_now + timedelta(hours=8)
    return beijing_time

# 自定义日志格式器
class BeijingFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        utc_dt = datetime.fromtimestamp(record.created, timezone.utc)
        bj_dt = utc_dt + timedelta(hours=8)
        if datefmt: return bj_dt.strftime(datefmt)
        return bj_dt.strftime('%Y-%m-%d %H:%M:%S')

handler = logging.StreamHandler()
handler.setFormatter(BeijingFormatter(
    fmt='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
))

logger = logging.getLogger("FundAdvisor")
logger.setLevel(logging.INFO)
if logger.hasHandlers(): logger.handlers.clear()
logger.addHandler(handler)

def retry(retries=3, delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if i < retries:
                        wait_time = delay * (1 + i)
                        logger.warning(
                            f"⚠️ [{func.__name__}] 失败: {str(e)[:100]}... "
                            f"| 重试 {i+1}/{retries}, 等待 {wait_time}s"
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(f"❌ [{func.__name__}] 彻底失败: {e}")
                        return None 
            return None
        return wrapper
    return decorator

def send_email(subject, content):
    sender = os.getenv("MAIL_USER")
    password = os.getenv("MAIL_PASS")
    
    if not sender or not password:
        logger.warning("未配置邮件账户，跳过发送。")
        return

    receiver = sender
    message = MIMEText(content, 'html', 'utf-8')
    message['From'] = f"Fund Advisor <{sender}>"
    message['To'] = receiver
    
    bj_time_str = get_beijing_time().strftime("%m-%d %H:%M")
    message['Subject'] = Header(f"[{bj_time_str}] {subject}", 'utf-8')

    try:
        if "qq.com" in sender:
            server = smtplib.SMTP_SSL("smtp.qq.com", 465)
        elif "163.com" in sender:
            server = smtplib.SMTP_SSL("smtp.163.com", 465)
        elif "gmail.com" in sender:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL("smtp.163.com", 465)

        server.login(sender, password)
        server.sendmail(sender, receiver, message.as_string())
        server.quit()
        logger.info("📧 邮件发送成功！")
        
    except Exception as e:
        logger.error(f"邮件发送失败: {e}")
