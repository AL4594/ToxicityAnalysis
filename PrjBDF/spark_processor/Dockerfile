# spark_processor/Dockerfile

FROM apache/spark:3.4.1

# ----------------- Démarrage des corrections -----------------

# Ajout des dépendances minimales (pyspark, kafka-python-ng)
WORKDIR /app
COPY requirements.txt .

# Temporairement, passez à l'utilisateur root pour garantir les permissions
USER root 

# Installe les dépendances Python (Maintenant avec les permissions de root)
RUN pip install --no-cache-dir -r requirements.txt

# Correction du PATH (maintenue)
ENV PATH="/opt/spark/bin:${PATH}"

# ----------------- Fin des corrections -----------------

# Copie le code source (ex: spark_processor.py)
COPY . .