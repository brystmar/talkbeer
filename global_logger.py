import logging
import sys
from os import path, mkdir

basedir = path.abspath(path.dirname(__file__))
local = '/documents/dev/' in basedir.lower() or 'pycharm' in basedir.lower()

# initialize logging
if local:
    print("Running locally: http://localhost:5000")

    log_dir = 'logs' if local else 'tmp'
    if not path.exists(log_dir):
        mkdir(log_dir)
    log_file = '{dir}/syslog.log'.format(dir=log_dir)

    logging.basicConfig(filename=log_file, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S',
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    logger = logging.getLogger(__name__)
else:
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S',
                        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
logger.info("\n=======================================\n\n")
logger.info("Global logging initialized!")
logger.info("local = {}".format(local))
# print("local = {}".format(local))
print("__file__ = {}".format(__file__))
