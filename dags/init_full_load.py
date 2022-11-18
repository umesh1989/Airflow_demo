"""This is the main file which controls the different tasks in workflow of complete load."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from helper.download_data import Download_data
from helper.db_util import DbUtilps
from helper.process_data_to_insert import ProcessData
import logging
import datetime


"""below method will download the files from specified location in the url"""
def download_data():
    logging.info('starting the data download')
    try:
        dd = Download_data()
        res = dd.init_download()
        logging.info('data download has completed with exit code {}'.format(res))
    except Exception as e:
        logging.exception('data download has failed')


"""below method is to bring up tables that are necessary for execution."""
def prepare_db():
    logging.info('starting to create table in Db if table not present')
    try:
        dbu = DbUtilps()
        res = dbu.prepare_data_store()
        del dbu
        logging.info('preparing db process has completed with exit code {}'.format(res))
    except Exception as e:
        logging.exception('tables prep has failed')


"""below method is to read the downloaded zip files and read data from them to insert into meta and reviews tables"""
def db_processing():
    logging.info('strating data insertion')
    try:
        pd = ProcessData()
        dbu = DbUtilps()
        ret_data = pd.prepare_auto()
        ins_meta = dbu.insert_data('meta',ret_data[0])
        ins_rev = dbu.insert_data('review', ret_data[1])
        del pd
        del dbu
        logging.info('data has been inserted in the tables')
    except Exception as e:
        logging.exception('data insertion has failed')


"""below method is to caluclate and store facts from meta and review tables. """
def prepare_facts():
    logging.info('preparing fact table')
    try:
        dbu = DbUtilps()
        res = dbu.prepare_facts()
        print(res)
        del dbu
        logging.info('fact table has been populated')
    except Exception as e:
        logging.exception('process failed while preparing fact table')


with DAG("init_full_load", start_date=datetime.datetime(2021, 1, 1),schedule_interval='0 */8 * * *', catchup=False) as dag:

    data_download = PythonOperator(task_id="data_download", python_callable=download_data)
    prepare_data_store = PythonOperator(task_id="prepare_data_store", python_callable=prepare_db)
    process_postgre = PythonOperator(task_id="process_postgre",python_callable=db_processing)
    process_facts = PythonOperator(task_id="process_facts", python_callable=prepare_facts)


    [data_download >> prepare_data_store >> process_postgre >> process_facts]

