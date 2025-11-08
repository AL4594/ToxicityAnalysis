FROM bitnami/spark:3-python
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY spark_processor.py /app/spark_processor.py