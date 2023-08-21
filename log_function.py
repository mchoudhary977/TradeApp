import logging
import datetime as dt 

log_file = f"logs/trade_app_{dt.datetime.now().strftime('%Y%M%d')}.log"

# logging.basicConfig(filename=log_file,level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(filename='mylog.log',level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def write_log(file_name, log_type, log_msg):
    message = f"{file_name} - {log_msg}"
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