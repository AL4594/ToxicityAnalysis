import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import nltk
from nltk.corpus import stopwords
import string
from collections import Counter


MONGO_URI = "mongodb://mongo:27017/"
MONGO_DATABASE = "toxicity_db"
MONGO_COLLECTION = "raw_comments"

@st.cache_resource
def download_nltk_data():
    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords')
download_nltk_data()

@st.cache_resource
def get_mongo_client():
    """Initialise la connexion MongoDB."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        return client
    except Exception as e:
        st.error(f"Erreur de connexion à MongoDB: {e}. Assurez-vous que le service 'mongo' est démarré.")
        return None

client = get_mongo_client()
st.set_page_config(layout="wide")
st.title("Dashboard d'Analyse de Toxicité des Flux Sociaux ☣️")

if client:
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

   
    @st.cache_data(ttl=60) 
    def load_data():
        """Charge toutes les données nécessaires depuis MongoDB."""
        try:
            cursor = collection.find(
                {},
                {'_id': 0, 'source': 1, 'text_content': 1, 'timestamp': 1, 'author': 1, 'metadata': 1, 'toxicity_score': 1}
            )
            df = pd.DataFrame(list(cursor))

            if df.empty:
                return df

            df['datetime'] = pd.to_datetime(df['timestamp'], errors='coerce')
            
            if 'metadata' in df.columns:
                df['post_title'] = df['metadata'].apply(lambda x: x.get('post_title') if isinstance(x, dict) else None)
                df['post_id'] = df['metadata'].apply(lambda x: x.get('post_id') if isinstance(x, dict) else None)
                df['post_title'] = df['post_title'].fillna(df['post_id'])
                df.drop(columns=['metadata'], inplace=True)

            return df
        
        except Exception as e:
            st.error(f"Erreur de lecture Pymongo: {e}.")
            return pd.DataFrame()

    df = load_data()
    
    if df.empty:
        st.info("La collection de données est vide. Veuillez patienter pendant que les producteurs et Spark traitent les données.")
        st.stop()

    
    st.sidebar.header("Filtres d'Analyse")
    all_sources = df['source'].unique()
    selected_sources = st.sidebar.multiselect(
        "Sélectionner les Sources:", 
        options=all_sources, 
        default=list(all_sources)
    )
    
    if not selected_sources:
        df_filtered = df.copy()
    else:
        df_filtered = df[df['source'].isin(selected_sources)]
    
    if df_filtered.empty:
        st.info("Aucune donnée pour les filtres sélectionnés.")
        st.stop()
        
    if 'toxicity_score' not in df_filtered.columns:
        st.warning("La colonne 'toxicity_score' n'est pas encore arrivée de Spark. Les analyses de toxicité sont désactivées.")
        df_filtered['toxicity_score'] = 0.0 
        has_toxicity_data = False
    else:
        has_toxicity_data = True
        df_filtered['toxicity_score'] = df_filtered['toxicity_score'].fillna(0.0)

    
    total_comments = len(df_filtered)
    total_authors = df_filtered['author'].nunique()
    
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(label="Total des Entrées (Commentaires/Threads)", value=f"{total_comments:,}", delta="Dans la sélection")
    with col2:
        st.metric(label="Auteurs Uniques Détectés", value=f"{total_authors:,}", delta_color="off")
    with col3:
        if has_toxicity_data:
            avg_score = df_filtered['toxicity_score'].mean()
            st.metric(label="Score de Toxicité Moyen", value=f"{avg_score:.2%}")
        else:
            st.metric(label="Score de Toxicité Moyen", value="N/A")

    st.markdown("---")

    
    st.header("Analyse de la Densité de Publication (Dernières 48H)")
    
    df_time = df_filtered.copy()
    cutoff_date = datetime.now() - timedelta(hours=48)
    df_time = df_time[df_time['datetime'] >= cutoff_date]
    
    if df_time.empty:
        st.info("Aucune donnée de publication dans les dernières 48 heures.")
    else:
        df_time['DateHeure'] = df_time['datetime'].dt.floor('H')
        time_series = df_time.groupby(['DateHeure', 'source']).size().reset_index(name='Volume')
        
        fig_time = px.bar(time_series, x='DateHeure', y='Volume', color='source',
                           title='Volume d\'Entrées par Heure (Dernières 48H)',
                           labels={'DateHeure': 'Date et Heure', 'Volume': 'Volume d\'Entrées'},
                           template='plotly_white')
        st.plotly_chart(fig_time, use_container_width=True)

    
    if has_toxicity_data:
        st.header("Analyse de la Toxicité ☣️")
        col_tox_1, col_tox_2 = st.columns(2)
        
        with col_tox_1:
            st.subheader("Communautés les plus toxiques")
            df_top_toxic = df_filtered.groupby('post_title')['toxicity_score'].mean().reset_index(name='Score Moyen')
            df_top_toxic = df_top_toxic.sort_values(by='Score Moyen', ascending=False).head(15)
            
            fig_tox_posts = px.bar(df_top_toxic, x='post_title', y='Score Moyen', 
                                 color='post_title', 
                                 title='Top 15 Posts/Vidéos par Toxicité Moyenne',
                                 template='plotly_dark')
            st.plotly_chart(fig_tox_posts, use_container_width=True)

        with col_tox_2:
            st.subheader("Auteurs les plus toxiques")
            author_counts = df_filtered['author'].value_counts()
            authors_to_keep = author_counts[author_counts >= 3].index
            
            df_toxic_authors = df_filtered[df_filtered['author'].isin(authors_to_keep)]
            df_toxic_authors = df_toxic_authors.groupby('author')['toxicity_score'].mean().reset_index(name='Score Moyen')
            df_toxic_authors = df_toxic_authors.sort_values(by='Score Moyen', ascending=False).head(10)
            
            fig_tox_authors = px.bar(df_toxic_authors, x='author', y='Score Moyen', 
                                     color='author', 
                                     title='Top 10 Auteurs Toxiques (min. 3 messages)',
                                     template='plotly_dark')
            st.plotly_chart(fig_tox_authors, use_container_width=True)
    
    st.markdown("---")

    
    st.header("Analyse des Tendances par Communauté 🔍")

    if has_toxicity_data:
        df_top_toxic_list = df_filtered.groupby('post_title')['toxicity_score'].mean().reset_index()
        df_top_toxic_list = df_top_toxic_list.sort_values(by='toxicity_score', ascending=False)
        
        selected_post_title = st.selectbox(
            "Choisissez une communauté (Post/Vidéo) à analyser:",
            options=df_top_toxic_list['post_title'].unique()
        )
        
        user_parasites = st.text_input(
            "Mots parasites à ignorer (séparés par une virgule):", 
            "i'm,it's,im,its"
        )
        
        if selected_post_title:
            df_community = df_filtered[df_filtered['post_title'] == selected_post_title]
            
            
            stop_words = set(stopwords.words('english') + stopwords.words('french') + list(string.punctuation))
            
            
            custom_parasites = {
                
                'like', 'get', 'one', 'also', 'would', 'could', 'got', 'really', 'even', 'people',
                'think', 'know', 'see', 'going', 'make', 'time', 'still', 'much', 'way', 'thing',
                'things', 'something', 'nothing', 'many', 'good', 'bad', 'new', 'old', 'lol', 'lmao',
                'https', 'http', 'www', 'com', 'org', 'net',
                
                'gt', 'lt', 'amp', 'quot', 'nbsp', 
                
                "i'm", "it's", "don't", "im", "its", "you're", "he's", "she's", "they're", "we're",
                "i've", "dont", "can't", "wont", "isn't", "aren't", "wasn't", "weren't", "c'est",
                "j'ai", "t'as", "l'a", "qu'il", "n'est", "cest",
                
                'reddit', 'youtube', 'comment', 'post', 'video', 'thread', 'subreddit', 'user',
                'reply', 'op', 'deleted', 'removed', '[deleted]', '[removed]', 'youtu', 'be'
            }
            stop_words.update(custom_parasites)
            
            
            if user_parasites:
                stop_words.update([word.strip().lower() for word in user_parasites.split(',')])

            
            all_text = " ".join(df_community['text_content'].str.lower())
            
            
            MIN_WORD_LENGTH = 3 
            
            words = []
            for word in all_text.split():
                
                if len(word) < MIN_WORD_LENGTH:
                    continue
                
                
                if word in stop_words:
                    continue
                    
                
                if word.isdigit():
                    continue
                
            
                words.append(word)
                
            word_counts = Counter(words)
            df_word_counts = pd.DataFrame(word_counts.most_common(20), columns=['Mot', 'Fréquence'])
            
            
            fig_word_count = px.bar(df_word_counts, x='Mot', y='Fréquence', 
                                    color='Fréquence', 
                                    title=f"Mots les plus fréquents pour '{selected_post_title}'",
                                    template='plotly_dark')
            st.plotly_chart(fig_word_count, use_container_width=True)
    else:
        st.info("Données de toxicité requises pour l'analyse des tendances.")
        
    st.markdown("---")

    
    st.header("Analyse de l'Activité (Volume)")
    col_auth, col_post = st.columns(2)

    with col_auth:
        st.subheader("Top 10 Auteurs par Volume")
        df_top_authors = df_filtered.groupby('author').size().reset_index(name='Contributions')
        df_top_authors = df_top_authors.sort_values(by='Contributions', ascending=False).head(10)
        
        fig_authors = px.bar(df_top_authors, x='author', y='Contributions', 
                             color='Contributions', 
                             title='Auteurs les plus actifs (Top 10)',
                             template='plotly_dark')
        st.plotly_chart(fig_authors, use_container_width=True)

    with col_post:
        st.subheader("Top 10 Posts/Threads par Volume de Commentaires")
        df_top_posts = df_filtered.groupby('post_title').size().reset_index(name='Nombre d\'Entrées')
        df_top_posts = df_top_posts.sort_values(by='Nombre d\'Entrées', ascending=False).head(10)

        fig_posts = go.Figure(data=[go.Pie(
            labels=df_top_posts['post_title'], 
            values=df_top_posts['Nombre d\'Entrées'], 
            hole=.3,
            name="Posts"
        )])
        fig_posts.update_layout(title_text="Répartition des Contributions par Post Source")
        st.plotly_chart(fig_posts, use_container_width=True)

  
    st.subheader("Aperçu des Données Filtrées (Derniers 50)")
    
    cols_to_show = ['source', 'datetime', 'author', 'text_content', 'post_title', 'toxicity_score']
    cols_exist = [col for col in cols_to_show if col in df_filtered.columns]
    
    if 'toxicity_score' in df_filtered.columns:
        df_display = df_filtered.copy()
        
        df_display['toxicity_score'] = df_display['toxicity_score'].map('{:,.2%}'.format)
        st.dataframe(df_display[cols_exist].sort_values(by='datetime', ascending=False).head(50))
    else:
        st.dataframe(df_filtered[cols_exist].sort_values(by='datetime', ascending=False).head(50))

else:
    st.error("Impossible d'établir la connexion à MongoDB. Veuillez vérifier la configuration.")