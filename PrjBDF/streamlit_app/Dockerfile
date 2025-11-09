# Utilise une image Python légère
FROM python:3.9-slim

# Expose le port par défaut de Streamlit
EXPOSE 8501

# Met à jour pip et installe les packages requis
WORKDIR /app
COPY requirements.txt .

# Installe les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copie le code source (ex: app.py)
COPY . .

# Commande de lancement par défaut pour Streamlit
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
