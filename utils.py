import time
import smtplib
import logging
import os
from email.mime.text import MIMEText
from email.utils import formataddr # 【新增】专门处理发件人格式
from functools import wraps

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
    mail_user = os.getenv("MAIL_USER")
    mail_pass = os.getenv("MAIL_PASS")
    
    if not mail_user or not mail_pass:
        logger.warning("未配置邮箱账号密码，跳过发送")
        return

    try:
        # 构建邮件
        message = MIMEText(content, 'plain', 'utf-8')
        
        # 【关键修复】QQ邮箱必须使用这种标准格式： 昵称 <邮箱地址>
        message['From'] = formataddr(["AI基金投顾", mail_user])
        message['To'] = formataddr(["我", mail_user])
        message['Subject'] = subject

        # 连接 QQ 邮箱服务器
        smtpObj = smtplib.SMTP_SSL('smtp.qq.com', 465)
        smtpObj.login(mail_user, mail_pass)
        # 注意：sendmail 的第一个参数 (from) 必须和 login 的账号一致
        smtpObj.sendmail(mail_user, [mail_user], message.as_string())
        smtpObj.quit()
        logger.info("邮件发送成功 📧")
    except Exception as e:
        logger.error(f"无法发送邮件: {e}")
