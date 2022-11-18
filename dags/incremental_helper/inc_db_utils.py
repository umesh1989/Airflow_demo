import psycopg2
import logging
from get_senti import CommonUtil

class IncDbUtil:
    def __init__(self):
        self.meta_table="create table IF NOT EXISTS inc_product_meta(asin varchar(20) not null, title text, description text, price numeric, primary key (asin))"
        self.review_table="create table IF NOT EXISTS inc_review_raw (review_id serial not null , reviewer_id varchar(30), asin varchar(20), review_text text, rating numeric, review_time date, unix_reviewtm integer, primary key(review_id))"
        self.fact_table = "create table IF NOT EXISTS inc_prod_facts(asin varchar(20),tot_pur numeric, total_earning numeric)"
        self.job_stats = "create table IF NOT EXISTS incremental_data_load_stats(load_id serial, last_fetch numeric, src_table varchar(30), primary key(load_id));"

    def create_connection(self):
        """for now connection credentials are hardcoded, common config file can be used to the same purpose later"""
        user = 'airflow'
        passwrd = 'airflow'
        conn = psycopg2.connect(
            database="best_seller", user=user, password=passwrd, host='postgres', port='5432'
        )
        conn.autocommit = True
        return conn

    def check_last_run(self):

        conn = self.create_connection()
        query_last_fetch = "select last_fetch from incremental_data_load_stats where src_table ='meta' " \
                           "order by load_id desc limit 1 "
        cursor = conn.cursor()
        cursor.execute(query_last_fetch)
        res = cursor.fetchone()
        cursor.close()
        conn.close()
        print('data from stats table')
        if res:
            print(res)
            return res[0]
        else:
            return 0

    def prepare_data_store(self):
        conn = self.create_connection()
        cursor = conn.cursor()
        cursor.execute(self.meta_table)
        cursor.execute(self.review_table)
        cursor.execute(self.fact_table)
        cursor.execute(self.job_stats)
        conn.commit()
        conn.close()
        cursor.close()
        return 0

    def fetch_meta_data(self, offset):
        offset = offset
        limit = 20
        offset_store = offset+limit
        try:
            update_job_sats="insert into incremental_data_load_stats(last_fetch, src_table) values ({},'meta');"
            query = "insert into inc_product_meta select * from product_meta order by asin  offset {} limit {}"
            query = query.format(offset,limit)
            print('insert query', query)
            conn = self.create_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            update_job_sats=update_job_sats.format(offset_store)
            cursor.execute(update_job_sats)
            cursor.close()
            conn.close()
            return 0
        except Exception as e:
            logging.exception('incremental meta load has failed')

    def populate_review_data(self):
        query = "select a.reviewer_id, a.asin, a.review_text, a.rating, a.review_time, a.unix_reviewtm from (select rr.reviewer_id, rr.asin as asin, rr.review_text, rr.rating, rr.review_time, rr.unix_reviewtm from review_raw rr inner join inc_product_meta pm on pm.asin=rr.asin) a left join inc_review_raw irr on a.asin = irr.asin where irr.asin is null"
        print(query)
        conn = self.create_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        print(res)
        insert_query = "insert into inc_review_raw(reviewer_id, asin, review_text, rating, review_time, unix_reviewtm) " \
                       "values (%s,%s,%s,%s,%s,%s) "
        cursor.executemany(insert_query,res)
        cursor.close()
        conn.close()

    def prepare_facts(self):
        conn = self.create_connection()
        cursor = conn.cursor()
        truncate_query = "truncate table prod_facts"
        fact_query = "insert into inc_prod_facts select asin,tot_pur,tot_pur*price as total_earning from ( select pm.asin,count(*) as tot_pur, pm.price from inc_product_meta pm join inc_review_raw rr on pm.asin=rr.asin group by 1,3) a"
        cursor.execute(truncate_query)
        cursor.execute(fact_query)
        conn.commit()
        cursor.close()
        conn.close()








