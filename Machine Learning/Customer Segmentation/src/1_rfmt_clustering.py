import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import sqlalchemy

# Set environment variables for optimized computation
os.environ["OMP_NUM_THREADS"] = '4'

# Database connection setup
def get_db_connection():
    DATABASE_URL = "postgresql://user:password@host:port/database"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return engine

# Fetch data from the database
def fetch_data():
    query = """
        SELECT client_id, last_purchase_date, purchase_count, total_spent, tenure
        FROM transactions_summary;
    """
    engine = get_db_connection()
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

# Data preprocessing
def preprocess_data(df):
    df = df.drop_duplicates(subset=['client_id'], keep='first')
    df.fillna(0, inplace=True)
    return df

# RFMT feature engineering
def compute_rfmt(df):
    df['recency'] = (pd.to_datetime('today') - pd.to_datetime(df['last_purchase_date'])).dt.days
    df.rename(columns={'purchase_count': 'frequency', 'total_spent': 'monetary', 'tenure': 'tenure'}, inplace=True)
    return df[['client_id', 'recency', 'frequency', 'monetary', 'tenure']]

# Perform K-Means clustering
def perform_clustering(df, n_clusters=4):
    scaler = StandardScaler()
    features = ['recency', 'frequency', 'monetary', 'tenure']
    df_scaled = scaler.fit_transform(df[features])
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df['cluster'] = kmeans.fit_predict(df_scaled)
    return df, kmeans

# Save results to the database
def save_to_db(df):
    engine = get_db_connection()
    df.to_sql("rfmt_clustered_data", engine, if_exists="replace", index=False)

# Main function
def main():
    df = fetch_data()
    df = preprocess_data(df)
    df_rfmt = compute_rfmt(df)
    clustered_df, model = perform_clustering(df_rfmt)
    
    print(clustered_df.head())
    save_to_db(clustered_df)

if __name__ == "__main__":
    main()
