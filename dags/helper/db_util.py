"""this is common db utility file to control all db operations"""

import psycopg2
import logging

class DbUtilps:
    """declaring all the static queries in init method, later all this can be moved to common input file"""
    def __init__(self):
        self.meta_query = "insert into product_meta values (%s,%s,%s,%s) ON CONFLICT (asin) DO NOTHING"
        self.review_query = "insert into review_raw(reviewer_id,asin,review_text,rating,review_time,unix_reviewtm) values " \
                "(%s,%s,%s,%s,%s,%s)"

        self.meta_table = "create table IF NOT EXISTS product_meta(asin varchar(20) not null, title text,description text, price numeric, primary key (asin))"

        self.revie_table = 'create table IF NOT EXISTS review_raw (review_id serial not null , reviewer_id varchar(30),' \
                           ' asin varchar(20), review_text text, rating numeric, review_time date, ' \
                           'unix_reviewtm integer, primary key(review_id))'
        self.prod_fact_table = 'create table IF NOT EXISTS prod_facts(asin varchar(20),tot_pur numeric, total_earning numeric)'

    """method to destroy class objects once all the processing is complete"""
    def __del__(self):
        pass

    def create_connection(self):
        """for now connection credentials are hardcoded, common config file can be used to the same purpose later"""
        user='airflow'
        passwrd='airflow'
        conn = psycopg2.connect(
            database="best_seller", user=user, password=passwrd, host='postgres', port='5432'
        )
        conn.autocommit = True
        return conn

    def insert_data(self,data_type,data):
        query=''
        conn = self.create_connection()
        cursor = conn.cursor()
        if data_type == 'meta':
            query = self.meta_query
        else:
            query = self.review_query
        logging.info('inserting {}'.format(data_type))
        cursor.executemany(query, data)
        conn.commit()
        conn.close()
        cursor.close()

    def prepare_data_store(self):
        conn = self.create_connection()
        cursor = conn.cursor()
        cursor.execute(self.meta_table)
        cursor.execute(self.revie_table)
        cursor.execute(self.prod_fact_table)
        conn.commit()
        conn.close()
        cursor.close()
        return 0

    def prepare_facts(self):
        conn = self.create_connection()
        cursor = conn.cursor()
        truncate_query = "truncate table prod_facts"
        fact_query = "insert into prod_facts select asin,tot_pur,tot_pur*price as total_earning from ( select pm.asin,count(*) as tot_pur, pm.price from product_meta pm join review_raw rr on pm.asin=rr.asin group by 1,3) a"
        cursor.execute(truncate_query)
        cursor.execute(fact_query)
        conn.commit()
        cursor.close()
        conn.close()


if __name__ == "__main__":
    dbu = DbUtilps()
    data =[('0006428320','Six Sonatas For Two Flutes Or Violins, Volume 2 (#4-6)','Description not available','17.95'),
            ('0014072149','Double Concerto in D Minor By Johann Sebastian Bach. Edited By David Oistrach. For Violin I, Violin Ii and Piano Accompaniment. Urtext. Baroque. Medium. Set of Performance Parts. Solo Parts, Piano Reduction and Introductory Text. BWV 1043.','Composer: J.S. Bach.Peters Edition.For two violins and pianos.','18.77'),
            ('01234','sample title','sample desc','123')]
    res = dbu.insert_data('meta',data)