

FROM apache/spark:3.4.1


WORKDIR /app
COPY requirements.txt .


USER root 


RUN pip install --no-cache-dir -r requirements.txt


ENV PATH="/opt/spark/bin:${PATH}"


COPY . .