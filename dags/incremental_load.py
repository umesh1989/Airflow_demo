"""this file contains the code for incremental load"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from incremental_helper.inc_db_utils import IncDbUtil
import logging
import datetime


def prepare_db():
    logging.info('starting to create table in Db if table not present')
    try:
        inc = IncDbUtil()
        res = inc.prepare_data_store()
        del inc
        logging.info('preparing db process has completed with exit code{}'.format(res))
    except Exception as e:
        logging.exception('tables prep has failed')


def start_meta_fetch():
    inc = IncDbUtil()
    last_offset = inc.check_last_run()
    print('last offset',int(last_offset))
    meta_data_fetch = inc.fetch_meta_data(last_offset)
    logging.info('meta fetch is completed')

def start_review_fetch():
    inc = IncDbUtil()
    inc.populate_review_data()

def update_facts():
    inc = IncDbUtil()
    inc.prepare_facts()



with DAG("incremental_load", start_date=datetime.datetime(2021, 1, 1),schedule_interval='0 */8 * * *', catchup=False) as dag:
    prepare_data_store = PythonOperator(task_id="prepare_data_store", python_callable=prepare_db)
    fetch_meta_increment = PythonOperator(task_id="fetch_meta_increment", python_callable=start_meta_fetch)
    fetch_review_increment = PythonOperator(task_id="fetch_review_increment", python_callable=start_review_fetch)
    update_facts = PythonOperator(task_id="update_facts", python_callable=update_facts)

    [prepare_data_store >> fetch_meta_increment >> fetch_review_increment >> update_facts]