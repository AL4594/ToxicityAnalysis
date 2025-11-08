
toxicity_project/
├── docker-compose.yml
│
├── producers/
│   ├── reddit_producer.py
│   ├── youtube_producer.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── spark_processor/
│   ├── spark_processor.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── mongo_consumer/
│   ├── mongo_consumer.py
│   ├── Dockerfile
│   └── requirements.txt
│
└── streamlit_app/
    ├── streamlit_app.py
    ├── Dockerfile
    └── requirements.txt

Lancez Docker: Ouvrez un terminal à la root du projet et exécutez :

docker-compose up --build

Accédez aux services:

Dashboard Streamlit: http://localhost:8501

Spark Master UI: http://localhost:8080

MongoDB (avec un client comme Compass): mongodb://localhost:27017