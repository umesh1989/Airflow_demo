"""this method is to read, clean data from the downloaded zip files to be inserted in tables"""


import gzip
import ast
import logging
import os

class ProcessData:
    def __init__(self):
        self.input_path='/opt/airflow/ip_file/'

    def __del__(self):
        pass

    def prepare_meta(self,src):
        meta_to_store=[]
        with gzip.open(src, mode='r') as zf:
            for l in zf:
                l = l.decode('utf-8')
                dic_line = ast.literal_eval(l)
                if 'title' not in dic_line:
                    dic_line['title'] = 'Title not available'
                if 'description' not in dic_line:
                    dic_line['description'] = 'Description not available'
                if 'price' not in dic_line:
                    dic_line['price'] = 0
                meta_to_store.append(
                    (dic_line['asin'], dic_line['title'], dic_line['description'], dic_line['price']))
        logging.info('total records in meta file:- {}'.format(len(meta_to_store)))
        return meta_to_store

    def prepare_reviews(self, src):
        reviews = []
        with gzip.open(src, mode='r') as zf:
            for l in zf:
                l = l.decode('utf-8')
                dic_line = ast.literal_eval(l)
                review_time_parts = dic_line['reviewTime'].split(' ')
                review_time = review_time_parts[2] + "-" + review_time_parts[0] + '-' + review_time_parts[1][0:2]
                dic_line['reviewTime'] = review_time
                reviews.append((dic_line['reviewerID'], dic_line['asin'], dic_line['reviewText'],
                                dic_line['overall'],dic_line['reviewTime'], dic_line['unixReviewTime']))
        logging.info('total records in reviews file:- {}'.format(len(reviews)))
        return reviews

    def prepare_auto(self):
        data_list=[]
        files = os.listdir(self.input_path)
        for f in files:
            src = self.input_path+f
            if 'meta' in f:
                logging.info('fetching meta')
                res = self.prepare_meta(src)
                data_list.append(res)
            else:
                logging.info('fetching review')
                res = self.prepare_reviews(src)
                data_list.append(res)
        return data_list


if __name__ == "__main__":
    pd = ProcessData()
    res = pd.prepare_auto()
    print('res',res)