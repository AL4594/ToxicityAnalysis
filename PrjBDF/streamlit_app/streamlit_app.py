import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# --- Configuration MongoDB ---
MONGO_URI = "mongodb://mongo:27017/"
MONGO_DATABASE = "toxicity_db"
MONGO_COLLECTION = "raw_comments"

@st.cache_resource
def get_mongo_client():
    """Initialise la connexion MongoDB."""
    try:
        # Tente la connexion
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Vérifie l'état de la connexion
        client.admin.command('ping') 
        return client
    except Exception as e:
        st.error(f"Erreur de connexion à MongoDB: {e}. Assurez-vous que le service 'mongo' est démarré.")
        return None

client = get_mongo_client()
st.set_page_config(layout="wide")

st.title("Dashboard d'Analyse des Flux Sociaux 📊")

if client:
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    # ----------------------------------------------------
    # --- 1. Récupération et Préparation des Données ---
    # ----------------------------------------------------
    @st.cache_data(ttl=60) # Mise en cache des données pour 60 secondes
    def load_data():
        """Charge toutes les données nécessaires depuis MongoDB en utilisant Spark SQL.

        Comportement:
        - Tente d'initialiser une SparkSession et lit la collection MongoDB via
          le mongo-spark-connector. Crée une vue temporaire et exécute une
          requête `spark.sql` pour récupérer les champs nécessaires.
        - En cas d'erreur (Spark absent / connexion impossible), retombe en
          arrière vers la lecture avec `pymongo` (comme auparavant).
        """
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder \
                .appName("streamlit_mongo_reader") \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .getOrCreate()

            # Lecture depuis MongoDB via le connector et création d'une view
            mongo_uri = f"{MONGO_URI}{MONGO_DATABASE}.{MONGO_COLLECTION}"
            df_spark = spark.read.format("mongo").option("uri", mongo_uri).load()
            df_spark.createOrReplaceTempView("raw_comments")

            # Utilisation d'une requête spark.sql pour sélectionner les champs
            sql_df = spark.sql(
                "SELECT source, text_content, timestamp, author, metadata FROM raw_comments"
            )

            # Conversion en pandas pour exploitation dans Streamlit/Plotly
            pandas_df = sql_df.toPandas()

            if pandas_df.empty:
                return pandas_df

            # Normaliser le timestamp: si c'est du datetime, convertir en secondes
            if 'timestamp' in pandas_df.columns:
                if pandas_df['timestamp'].dtype == 'datetime64[ns]':
                    pandas_df['timestamp'] = pandas_df['timestamp'].view('int64') // 10**9

            # Extraction du post_id à partir de metadata (si présent)
            if 'metadata' in pandas_df.columns:
                pandas_df['post_id'] = pandas_df['metadata'].apply(lambda x: x.get('post_id') if isinstance(x, dict) else None)
                pandas_df.drop(columns=['metadata'], inplace=True)

            # Ajout de la colonne datetime pour la compatibilité avec le reste du code
            pandas_df['datetime'] = pd.to_datetime(pandas_df['timestamp'], unit='s', origin='unix', errors='coerce')

            return pandas_df

        except Exception as e:
            # Si Spark n'est pas disponible dans l'environnement Streamlit,
            # on retombe sur la lecture pymongo comme précédemment.
            st.warning(f"Spark unavailable or error reading with Spark: {e}. Falling back to pymongo.")
            cursor = collection.find(
                {},
                {'_id': 0, 'source': 1, 'text_content': 1, 'timestamp': 1, 'author': 1, 'metadata': 1}
            )
            df = pd.DataFrame(list(cursor))

            if df.empty:
                return df

            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', origin='unix', errors='coerce')

            if 'metadata' in df.columns:
                df['post_id'] = df['metadata'].apply(lambda x: x.get('post_id') if isinstance(x, dict) else None)
                df.drop(columns=['metadata'], inplace=True)

            return df

    df = load_data()
    
    if df.empty:
        st.info("La collection de données est vide. Veuillez démarrer vos producteurs Kafka/Spark.")
        st.stop()

    # --- Filtres Globaux ---
    st.sidebar.header("Filtres d'Analyse")
    selected_sources = st.sidebar.multiselect(
        "Sélectionner les Sources:", 
        options=df['source'].unique(), 
        default=df['source'].unique()
    )
    df_filtered = df[df['source'].isin(selected_sources)]
    
    # ----------------------------------------------------
    # --- 2. Statistiques Clés (KPIs) ---
    # ----------------------------------------------------
    
    total_comments = len(df_filtered)
    total_authors = df_filtered['author'].nunique()
    
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(label="Total des Entrées (Commentaires/Threads)", value=f"{total_comments:,}", delta="Dans la sélection")
    with col2:
        st.metric(label="Auteurs Uniques Détectés", value=f"{total_authors:,}", delta_color="off")
    with col3:
        # Calcul de la moyenne des mots par commentaire
        df_filtered['word_count'] = df_filtered['text_content'].apply(lambda x: len(str(x).split()))
        avg_words = df_filtered['word_count'].mean()
        st.metric(label="Moyenne Mots/Entrée", value=f"{avg_words:.1f}", delta_color="off")

    st.markdown("---")

    # ----------------------------------------------------
    # --- 3. Analyse Temporelle (Densité de Publication) ---
    # ----------------------------------------------------
    st.header("Analyse de la Densité de Publication 📈")
    
    df_time = df_filtered.copy()
    # Agrégation par heure pour la densité
    df_time['DateHeure'] = df_time['datetime'].dt.to_period('H').dt.to_timestamp()
    
    time_series = df_time.groupby(['DateHeure', 'source']).size().reset_index(name='Volume')
    
    fig_time = px.line(time_series, x='DateHeure', y='Volume', color='source',
                       title='Volume d\'Entrées par Heure',
                       labels={'DateHeure': 'Date et Heure', 'Volume': 'Volume d\'Entrées'},
                       template='plotly_white')
    st.plotly_chart(fig_time, use_container_width=True)

    # ----------------------------------------------------
    # --- Distribution par Source (YouTube vs Reddit) ---
    # ----------------------------------------------------
    st.header("Répartition par Source")
    try:
        dist = df_filtered['source'].value_counts().reset_index()
        dist.columns = ['source', 'count']

        fig_dist = px.pie(dist, names='source', values='count',
                         title="Répartition des Entrées par Source (YouTube vs Reddit)")
        st.plotly_chart(fig_dist, use_container_width=True)
    except Exception as e:
        st.warning(f"Impossible de générer le graphique de répartition: {e}")


    # ----------------------------------------------------
    # --- 4. Top Auteurs et Posts (Requêtes SQL Simulé) ---
    # ----------------------------------------------------
    col_auth, col_post = st.columns(2)

    with col_auth:
        st.subheader("Top 10 Auteurs par Volume")
        # Simule une requête SQL: SELECT author, count(*) as count GROUP BY author ORDER BY count DESC LIMIT 10
        df_top_authors = df_filtered.groupby('author').size().reset_index(name='Contributions')
        df_top_authors = df_top_authors.sort_values(by='Contributions', ascending=False).head(10)
        
        # Création d'un graphique à barres pour les auteurs
        fig_authors = px.bar(df_top_authors, x='author', y='Contributions', 
                             color='Contributions', 
                             title='Auteurs les plus actifs (Top 10)',
                             template='plotly_dark')
        st.plotly_chart(fig_authors, use_container_width=True)

    with col_post:
        st.subheader("Top 10 Posts/Threads (ID) par Commentaires")
        # Simule une requête SQL: SELECT post_id, count(*) as count GROUP BY post_id ORDER BY count DESC LIMIT 10
        df_top_posts = df_filtered.groupby('post_id').size().reset_index(name='Nombre d\'Entrées')
        df_top_posts = df_top_posts.sort_values(by='Nombre d\'Entrées', ascending=False).head(10)

        # Création d'un graphique en anneau (donut)
        fig_posts = go.Figure(data=[go.Pie(
            labels=df_top_posts['post_id'], 
            values=df_top_posts['Nombre d\'Entrées'], 
            hole=.3,
            name="Posts"
        )])
        fig_posts.update_layout(title_text="Répartition des Contributions par Post Source")
        st.plotly_chart(fig_posts, use_container_width=True)

    # ----------------------------------------------------
    # --- 5. Aperçu Détaillé ---
    # ----------------------------------------------------
    st.subheader("Aperçu des Données Filtrées")
    # Affiche le dataframe avec une sélection de colonnes pertinentes
    st.dataframe(df_filtered[['source', 'datetime', 'author', 'text_content', 'post_id']].head(50))
else:
    st.error("Impossible d'établir la connexion à MongoDB. Veuillez vérifier la configuration.")