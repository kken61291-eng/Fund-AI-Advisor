import time
import smtplib
import logging
import os
from email.mime.text import MIMEText
from email.header import Header
from functools import wraps

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def retry(retries=3, delay=2):
    """通用的重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"执行 {func.__name__} 失败 ({i+1}/{retries}): {e}")
                    last_exception = e
                    time.sleep(delay)
            logger.error(f"函数 {func.__name__} 最终执行失败。")
            raise last_exception
        return wrapper
    return decorator

def send_email(subject, content):
    """发送邮件通知 (QQ邮箱)"""
    mail_user = os.getenv("MAIL_USER") # 你的QQ邮箱
    mail_pass = os.getenv("MAIL_PASS") # 刚才获取的授权码
    
    if not mail_user or not mail_pass:
        logger.warning("未配置邮箱账号密码，跳过发送")
        return

    sender = mail_user
    receivers = [mail_user]  # 发给自己

    # 构建邮件
    message = MIMEText(content, 'plain', 'utf-8')
    message['From'] = Header("AI基金投顾", 'utf-8')
    message['To'] = Header("我", 'utf-8')
    message['Subject'] = Header(subject, 'utf-8')

    try:
        # 连接 QQ 邮箱服务器 (SSL加密, 端口465)
        smtpObj = smtplib.SMTP_SSL('smtp.qq.com', 465)
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        smtpObj.quit()
        logger.info("邮件发送成功")
    except smtplib.SMTPException as e:
        logger.error(f"无法发送邮件: {e}")
