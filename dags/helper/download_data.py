"""this file is intended to download data files. For now multithreading has been used. Later thread pools can be made
dynamic in case we need to fetch more file at the same time"""

import threading
import wget
import logging


class Download_data:
    def __init__(self):
        self.data_url = 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Musical_Instruments.json.gz'
        self.meta_data_url = 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Musical_Instruments.json.gz'

    def __del__(self):
        pass

    def data_download(self, url,file_name):
        try:
            wget.download(url,file_name)
        except Exception as ex:
            print(ex)

    """below method initiates data download. For now file have been hard coded, inputs can be taken as parameter in 
    later upgrades"""
    def init_download(self,link):
        logging.info('creating threads for data download')
        data = threading.Thread(target=self.data_download,
                                args=(self.data_url, '/opt/airflow/ip_file/reviews_Musical_Instruments.json.gz'))
        metadata = threading.Thread(target=self.data_download,
                                    args=(self.meta_data_url, '/opt/airflow/ip_file/meta_Musical_Instruments.json.gz'))
        data.start()
        metadata.start()
        data.join()
        metadata.join()
        return 0


if __name__ == '__main__':
    dd = Download_data()
    dd.init_download()


