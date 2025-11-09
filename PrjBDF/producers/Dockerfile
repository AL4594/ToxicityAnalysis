# Utilise une image Python légère
FROM python:3.9-slim

# Met à jour pip et installe les packages requis
WORKDIR /app
COPY requirements.txt .

# Installe les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copie le code source (ex: reddit_producer.py)
COPY . .

# Commande de lancement (sera surchargée par docker-compose)
CMD ["python", "reddit_producer.py"]