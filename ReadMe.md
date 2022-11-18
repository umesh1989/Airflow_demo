Below is the explanation of the task the was given to me by Bestseller.

As I was asked to create a pipeline that fetch data from json file, transform it into dimensional models and then load data into data store.

For demo I have used postgre to store data.

the data source mentioned in the assignment http://jmcauley.ucsd.edu/data/amazon/links.html has all the static file so, data can not be streamed from the mentioned source.

So for demo I have created two pipelines:
1. Full Load: 
   1. This pipelines downloads are hardcoded Musical_Instruments data set from the source because it was smaller in size. 
   2. Data source has two file, metadata and reviews.
   3. After downloading the zip files into ip_file dir under root_dir, pipeline read data from meta and reviews file and store it in the below tables:
      1. product_meta
      2. review_raw
   4. Post data insertion another scripts calculates the total quantity sold for a product and total amount of those sales.
      1.     best_seller=# select * from prod_facts limit 5;
                     asin    | tot_pur | total_earning 
                 ------------+---------+---------------
                 B00332VUWY |       4 |       2399.96
                 B001BS13GU |      22 |        2178.0
                 B0002EJP3C |       6 |         77.76
                 B000O78IRG |       1 |             0
                 B003A5UNP4 |       1 |          3.79
      2. here asin is the product id which is reference key from meta and review table. 
2. Incremental load:
   1. As by default data streaming and partial data load is not possible from the provided data source. I have used table created by full load pipeline to demonstrate batch data pull.
   2. Incremental load will take data from meta table to populate the incremental meta table.
   3. Since it is a demo I am fetching only 20 records per execution
   4. Post that on the basis of product id fetched in above step review data will be fetch from review_raw table into incremental review table for the products which are not present in the incremental review table.
      1. ex: we had product id A,B,C in incremental review and meta table. Post next execution meta table got D,E new product ids do now data for only D,E will be fetched from review table to incremental review table.
      2. Facts will be calculated same as full load.
3. Facts calculation:
   1. fact table is getting dropped and recreated at every calculation.

Both full and incremental loads are scheduled to run, after every 8 hours.

How to execute the code.
1. Put the code in one dir.
2. I have added two additional python libs so, do a docker build as follows:
   1. docker build . --tag extending_airflow:latest
3. Now run airflow-init:
   1. docker-compose up airflow-init
4. Now fire up Docker compose:
   1. docker-compose up
5. Delete files from ip_file
6. Login to postgre in docker and create db best_seller
7.select dags init_full_load and incremental_load.
   1. mark them active one by one, as if both are marked active together we may experience failure.

Above steps will run airflow with our modified docker image.

**Table Structure:**

create statements for full load:
1.     create table product_meta(asin varchar(20) not null, title text, description text, price numeric, primary key (asin));
2.     create table review_raw (review_id serial not null , reviewer_id varchar(30), asin varchar(20), review_text text, rating numeric, review_time date, unix_reviewtm integer, primary key(review_id));
3.     create table prod_facts(asin varchar(20),tot_pur numeric, total_earning numeric);

Create statements for incremental load:
1.     create table incremental_data_load_stats(load_id serial, last_fetch numeric, src_table varchar(30), primary key(load_id));
2.     create table inc_product_meta(asin varchar(20) not null, title text, description text, price numeric, primary key (asin));
3.     create table inc_review_raw (review_id serial not null , reviewer_id varchar(30), asin varchar(20), review_text text, rating numeric, review_time date, unix_reviewtm integer, primary key(review_id));
4.     create table inc_prod_facts(asin varchar(20),tot_pur numeric, total_earning numeric);

**Code Structure**
init_full_load.py is the dag node for full load and dags/helper has all the supporting files for execution.
Same is the structure of incremental_load.py and dags/incremental_helper

**Note and Next enhancements:** 
1. Please login in to postgres cli from docker and create db best_seller manually. I haven't automated the db creation. Rest all the required tables will be created by pipeline itself.
2. For logging and exception handling, default logging modules has been used and all the custom info/exceptions are being directed to airflow logs.
3. I had a plan to add sentiments of the reviews as well, as dimension in the review table, and the count of sentiments as fact in fact table. I have written a code in /incremental_helper/get_senti.py. But could not integrate it with the pipeline because of lack of time.
4. file download can be made more flexible by using thread pool. Currently, I am downloading only two files, hence made only two threads to download files.
5. Downloaded file could be moved to external storage like, S3 to archiving purpose.
6. Duplicates have been handled while inserting data in meta table.
7. Code has been modularized using classes, so individual part can be re-used.

Graph view of the dags has been stored in a dir name graph_dag in the root folder.

I have properly commented and put all the loggings/exceptions that seems adequate to me in the full load pipeline. Incremental load is almost same to full load hence I have not made comments and logging/exception handling in incremental load.
