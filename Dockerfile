FROM apache/airflow:2.4.1
COPY requirements.txt /rerequirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /rerequirements.txt
