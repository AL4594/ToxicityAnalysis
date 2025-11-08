import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import time

# --- Configuration ---
MONGO_SERVER = "mongo:27017" # Nom du service Docker
MONGO_DB = "toxicity_db"
MONGO_COLLECTION = "comments"

# --- Connexion à la base de données ---
@st.cache_resource
def get_mongo_client():
    client = MongoClient(f"mongodb://{MONGO_SERVER}/")
    return client

@st.cache_data(ttl=10) # Rafraîchir les données toutes les 10 secondes
def fetch_data(_client):
    db = _client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    # 1. Derniers commentaires toxiques
    toxic_comments = list(collection.find(
        {"toxicity_score": {"$gt": 0.8}},
        {"_id": 0, "text_content": 1, "toxicity_score": 1, "source_name": 1}
    ).limit(10).sort("timestamp", -1))
    
    # 2. Score moyen par source (Top 10)
    pipeline_source = [
        {"$group": {
            "_id": "$source_name",
            "avg_toxicity": {"$avg": "$toxicity_score"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 5}}}, # Au moins 5 commentaires
        {"$sort": {"avg_toxicity": -1}},
        {"$limit": 10}
    ]
    avg_by_source = list(collection.aggregate(pipeline_source))
    
    # 3. Evolution de la toxicité (1000 derniers points)
    timeline_data = list(collection.find(
        {},
        {"_id": 0, "timestamp": 1, "toxicity_score": 1}
    ).limit(1000).sort("timestamp", -1))

    # 4. Total des commentaires
    total_count = collection.count_documents({})

    return toxic_comments, avg_by_source, timeline_data, total_count

# --- Configuration de la page ---
st.set_page_config(page_title="Analyse de Toxicité", layout="wide")
st.title("🚨 Tableau de Bord - Analyse de Toxicité (Reddit & YouTube)")

# Connexion
client = get_mongo_client()

# Placeholder pour les métriques
placeholder = st.empty()

while True:
    # Récupérer les données
    toxic_comments, avg_by_source, timeline_data, total_count = fetch_data(client)
    
    with placeholder.container():
        # --- Métriques Clés ---
        kpi1, kpi2 = st.columns(2)
        kpi1.metric("Commentaires Analysés", f"{total_count:,}")
        
        avg_global = 0
        if timeline_data:
             avg_global = pd.DataFrame(timeline_data)["toxicity_score"].mean()
        kpi2.metric("Toxicité Moyenne Globale", f"{avg_global:.2%}")
        
        st.divider()

        # --- Graphiques ---
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Sources les plus Toxiques (Score moyen)")
            if avg_by_source:
                df_source = pd.DataFrame(avg_by_source).rename(columns={"_id": "Source"})
                fig_source = px.bar(df_source, x="avg_toxicity", y="Source", 
                                    orientation='h', title="Top 10 Sources par Toxicité")
                fig_source.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_source, use_container_width=True)
            else:
                st.info("En attente de données agrégées...")
        
        with col2:
            st.subheader("Derniers Commentaires Toxiques (Score > 0.8)")
            if toxic_comments:
                df_toxic = pd.DataFrame(toxic_comments)
                st.dataframe(df_toxic, use_container_width=True)
            else:
                st.info("Aucun commentaire hautement toxique détecté récemment.")
        
        st.divider()
        
        # --- Graphique temporel ---
        st.subheader("Évolution de la Toxicité (1000 derniers commentaires)")
        if timeline_data:
            df_timeline = pd.DataFrame(timeline_data)
            df_timeline['timestamp'] = pd.to_datetime(df_timeline['timestamp'], unit='s')
            # Calculer une moyenne mobile
            df_timeline = df_timeline.sort_values('timestamp')
            df_timeline['rolling_avg'] = df_timeline['toxicity_score'].rolling(window=50).mean()
            
            fig_timeline = px.line(df_timeline, x="timestamp", y=["toxicity_score", "rolling_avg"], 
                                   title="Toxicité au fil du temps")
            st.plotly_chart(fig_timeline, use_container_width=True)
        else:
            st.info("En attente de données temporelles...")

    # Rafraîchir toutes les 10 secondes
    time.sleep(10)