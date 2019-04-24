import datetime
import logging
import os
from sqlalchemy import create_engine, select
from models import Base
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# create file handler for logs
log_handler = logging.FileHandler('logs/migrate_sqlite.log')
log_handler.setLevel(logging.DEBUG)

# define logging format
log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_format)

# add the handlers to the logger
logger.addHandler(log_handler)

# set path for loading local .env variables
basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))

db_user = os.environ.get('DB_USER')
db_pw = os.environ.get('DB_PW')
db_name = os.environ.get('DB_NAME')
db_instance = os.environ.get('DB_INSTANCE')

cloud_server = 'postgresql+psycopg2://{user}:{pw}@/{db_name}'.format(user=db_user, pw=db_pw, db_name=db_name)
cloud_server += '?host=/cloudsql/{db_instance}'.format(db_instance=db_instance)

local_db = 'sqlite:///talkbeer.sqlite'
engine_lite = create_engine(local_db)
engine_cloud = create_engine(cloud_server)
logger.debug('Cloud DB: {}'.format(cloud_server))

done = []

logger.debug('Start reading db')
with engine_lite.connect() as conn_lite:
    with engine_cloud.connect() as conn_cloud:
        for table in Base.metadata.sorted_tables:
            print(table.schema, table.name)

            if table.schema == 'raw' and table.name == 'thread_page':
                logger.debug("Starting {table} @ {dt}".format(table=table.name, dt=datetime.datetime.now()))

                logger.debug('Start reading records from sqlite db')
                table.schema = 'main'
                data = [dict(row) for row in conn_lite.execute(select(table.c))]
                table.schema = 'raw'
                logger.debug('Finished reading records from sqlite db.  Total rows: {}'.format(len(data)))

                try:
                    logger.debug('Start writing to cloud db')
                    conn_cloud.execute(table.insert().values(data))
                    logger.debug('Finished writing to cloud db')
                except Exception:
                    logger.error('Error writing to cloud db', exc_info=True)

                logger.debug("Finished {table} @ {dt}".format(table=table.name, dt=datetime.datetime.now()))
