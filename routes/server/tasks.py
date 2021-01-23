import os
import time
import pandas as pd

from pyhive import presto
from celery import Celery
from datetime import datetime
from routes.server.sql_script import gen_sql

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://reids:6379")

@celery.task(name="create_task")
def create_task(task_type):
    time.sleep(int(task_type) * 10)
    print(f'[INFO - TASKTYPE] : {task_type}')

    # # # TODO Declare Presto connection
    # presto_conn = presto.connect(
    #                                 host = 'bdp-e2e-presto.wdc.com',
    #                                 port = 8446,
    #                                 protocol = 'https',
    #                                 catalog = 'hive',
    #                                 username = 'rujikorn.ngoensaard@wdc.com',
    #                                 requests_kwargs = {
    #                                     'verify': os.path.join(os.getcwd(), 'routes', 'server', 'db_driver', 'WDC_CA_bundle.pem')
    #                                     }
    #                             )

    # print(f'[INFO - {datetime.now()}] -> @ Start pull data from Presto')

    # # TODO Sending SQL CMD
    # df = pd.read_sql(gen_sql(), presto_conn)

    # print(df.head())

    return True