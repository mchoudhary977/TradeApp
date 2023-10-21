import logging
import datetime as dt

log_file = f"logs/trade_app_{dt.datetime.now().strftime('%Y%m%d')}.log"
# log_file = f"trade_app_{dt.datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(filename=log_file,level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.basicConfig(filename='mylog.log',level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def printLog(log_type, log_msg):
    now = dt.datetime.now()
    log = 'INFO' if log_type == 'i' else ('ERROR' if log_type == 'e' else 'DEBUG')
    print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - {log} - {log_msg}")

    with open(log_file,'a') as file:
         print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - {log} - {log_msg}",file=file)

    message = f"{log_msg}"
    if log_type == 'd':
        logging.debug(message)
    elif log_type == 'i':
        logging.info(message)
    elif log_type == 'w':
        logging.warning(message)
    elif log_type == 'e':
        logging.error(message)
    elif log_type == 'c':
        logging.critical(message)


# write_log('abc.xy','i','logging error')