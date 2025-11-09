import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px

# --- Configuration MongoDB ---
MONGO_URI = "mongodb://mongo:27017/"
MONGO_DATABASE = "toxicity_db"
MONGO_COLLECTION = "raw_comments"

@st.cache_resource # Mise en cache de la connexion
def get_mongo_client():
    """Initialise la connexion MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        return client
    except Exception as e:
        st.error(f"Erreur de connexion à MongoDB: {e}")
        return None

client = get_mongo_client()

st.title("Dashboard de Collecte de Commentaires (Reddit & YouTube) 🌐")

if client:
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    # --- 1. Statistiques Globales ---
    total_comments = collection.count_documents({})
    st.header(f"Total des Commentaires Collectés: {total_comments:,}")

    # --- 2. Distribution par Source (Reddit vs YouTube) ---
    st.subheader("Distribution des Commentaires par Source")
    
    pipeline_source_count = [
        {"$group": {"_id": "$source", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    df_source = pd.DataFrame(list(collection.aggregate(pipeline_source_count)))
    
    if not df_source.empty:
        df_source.rename(columns={'_id': 'Source', 'count': 'Nombre'}, inplace=True)
        fig = px.bar(df_source, x='Source', y='Nombre', 
                     color='Source', title='Volume par Source')
        st.plotly_chart(fig)

    # --- 3. Aperçu des Derniers Commentaires ---
    st.subheader("Aperçu des 10 Derniers Commentaires Bruts")
    
    recent_comments = list(collection.find().sort("timestamp", -1).limit(10))
    df_recent = pd.DataFrame(recent_comments)
    
    if '_id' in df_recent.columns:
        df_recent.drop(columns=['_id'], inplace=True) # Retirer l'ID Mongo
    
    st.dataframe(df_recent)

else:
    st.warning("Veuillez vérifier que le service 'mongo' est démarré et accessible.")