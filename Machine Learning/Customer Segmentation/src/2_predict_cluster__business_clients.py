import os
import numpy as np
import pandas as pd
import pickle
import sqlalchemy
from sklearn.preprocessing import StandardScaler, LabelEncoder

# Set environment variables for optimized computation
os.environ["OMP_NUM_THREADS"] = '4'

# Database connection setup
def get_db_connection():
    DATABASE_URL = "postgresql://user:password@host:port/database"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return engine

# Fetch new client data from the database
def fetch_new_clients():
    query = """
        SELECT client_id, age, income, region, industry, tenure 
        FROM new_clients;
    """
    engine = get_db_connection()
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

# Data preprocessing
def preprocess_data(df):
    df.fillna(0, inplace=True)
    label_encoders = {}
    categorical_features = ['region', 'industry']
    
    for col in categorical_features:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])
        label_encoders[col] = le
    
    scaler = StandardScaler()
    numerical_features = ['age', 'income', 'tenure']
    df[numerical_features] = scaler.fit_transform(df[numerical_features])
    
    return df, scaler, label_encoders

# Load trained model
def load_model(model_path="business_client_rfmt_cluster_model.pkl"):
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    return model

# Predict cluster assignments
def predict_clusters(df, model):
    features = ['age', 'income', 'region', 'industry', 'tenure']
    df['predicted_cluster'] = model.predict(df[features])
    return df

# Save predictions to the database
def save_predictions(df):
    engine = get_db_connection()
    df.to_sql("predicted_client_clusters", engine, if_exists="replace", index=False)

# Main function
def main():
    df = fetch_new_clients()
    df, scaler, label_encoders = preprocess_data(df)
    model = load_model()
    df = predict_clusters(df, model)
    
    print(df.head())
    save_predictions(df)

if __name__ == "__main__":
    main()
