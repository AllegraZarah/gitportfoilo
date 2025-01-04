# Import required Libraries
import os
os.environ["OMP_NUM_THREADS"] = '4'  # specified with the goal of improving the efficiency of the code

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Database Connection Configuration
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")  # Default to public schema if not specified

# Configuration Parameters
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "customer_transactions")  # Table containing transaction data
TARGET_TABLE = os.getenv("TARGET_TABLE", "customer_segments")      # Table to store results
ID_COLUMN = os.getenv("ID_COLUMN", "customer_id")                 # Primary identifier column
FEATURE_START_COL = 3  # Index where feature columns start

# Cluster Labels
CLUSTER_LABELS = {
    0: 'Segment A',
    1: 'Segment B'
}

def create_db_connection():
    """Create database connection using environment variables"""
    connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    return create_engine(connection_string)

def load_data(engine, table_name, schema):
    """Load data from database"""
    conn = engine.connect().execution_options(stream_results=True)
    return pd.read_sql_table(table_name, conn, schema)

def preprocess_data(df, feature_start_col):
    """Preprocess the data for clustering"""
    # Select feature columns
    feature_data = df.iloc[:, feature_start_col:]
    
    # Log transformation
    data_log = np.log(feature_data)
    data_log.replace([np.inf, -np.inf], 0, inplace=True)
    
    # Fill NA values
    for column in data_log.columns:
        data_log[column] = data_log[column].fillna(0)
    
    return data_log

def plot_elbow_curve(transformed_data, max_clusters=9):
    """Plot elbow curve to help determine optimal number of clusters"""
    num_clusters = list(range(1, max_clusters))
    sse = []
    
    for k in num_clusters:
        kmeans = KMeans(n_clusters=k)
        kmeans.fit(transformed_data)
        sse.append(kmeans.inertia_)
    
    plt.figure(figsize=(10, 6))
    plt.plot(num_clusters, sse)
    plt.xlabel('Number of Clusters')
    plt.ylabel('Sum of Squared Errors')
    plt.title('Elbow Method for Optimal k')
    plt.show()

def perform_clustering(data, n_clusters=2, random_state=1):
    """Perform KMeans clustering"""
    # Scale the data
    scaler = StandardScaler()
    transformed_data = scaler.fit_transform(data)
    
    # Fit KMeans model
    model = KMeans(n_clusters=n_clusters, random_state=random_state)
    model.fit(transformed_data)
    
    return model.labels_

def save_results(df, labels, engine, target_table, schema, id_column, label_mapping):
    """Save clustering results to database"""
    # Create results dataframe
    results_df = pd.DataFrame({
        id_column: df[id_column],
        'segment': labels
    })
    
    # Map numeric labels to meaningful names
    results_df['segment'] = results_df['segment'].map(label_mapping)
    
    # Save to database
    results_df.to_sql(target_table, engine, schema=schema, 
                      if_exists='replace', index=False)

def main():
    # Create database connection
    engine = create_db_connection()
    
    # Load data
    print("Loading data...")
    df = load_data(engine, SOURCE_TABLE, DB_SCHEMA)
    
    # Preprocess data
    print("Preprocessing data...")
    processed_data = preprocess_data(df, FEATURE_START_COL)
    
    # Plot elbow curve
    print("Generating elbow curve...")
    plot_elbow_curve(processed_data)
    
    # Perform clustering
    print("Performing clustering...")
    labels = perform_clustering(processed_data)
    
    # Save results
    print("Saving results...")
    save_results(df, labels, engine, TARGET_TABLE, DB_SCHEMA, 
                ID_COLUMN, CLUSTER_LABELS)
    
    print("Clustering complete!")

if __name__ == "__main__":
    main()