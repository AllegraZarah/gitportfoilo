import os
os.environ["OMP_NUM_THREADS"] = '4'  # specified with the goal of improving the efficiency of the code

import psycopg2
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import itertools
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import random as python_random 
from sklearn.decomposition import PCA, FastICA
from sklearn.model_selection import (
    train_test_split, KFold, StratifiedKFold,
    RepeatedKFold, ShuffleSplit, StratifiedShuffleSplit
)
from scipy.stats import mode
from sklearn.feature_selection import SelectFromModel, RFECV, RFE
from sklearn.preprocessing import (
    LabelEncoder, StandardScaler, MinMaxScaler,
    PolynomialFeatures, Normalizer
)
from sklearn.metrics import (
    confusion_matrix, f1_score, roc_auc_score,
    mean_squared_error, accuracy_score, log_loss,
    classification_report
)
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import LogisticRegression, ElasticNet, RidgeClassifier, Lasso
from sklearn.ensemble import (
    IsolationForest, RandomForestClassifier, BaggingClassifier,
    ExtraTreesClassifier, GradientBoostingClassifier,
    AdaBoostClassifier, VotingClassifier, StackingClassifier
)
from catboost import CatBoostClassifier, Pool
from xgboost import XGBRFClassifier, XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import pickle

def create_db_connection():
    """Create database connection using environment variables"""
    load_dotenv()
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    
    connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    engine = create_engine(connection_string)
    return engine.connect().execution_options(stream_results=True)

def load_data(conn, table_name, schema):
    """Load data from database"""
    return pd.read_sql_table(table_name, conn, schema)

def prepare_data(data, id_column, target_column, date_column):
    """Prepare data for modeling"""
    # Save target and identifier
    target = data[target_column]
    unique_identifier = data[id_column]
    
    # Drop identifier
    processed = data.drop(id_column, axis=1)
    
    # Handle date column
    processed_date = processed[date_column]
    processed = processed.drop(date_column, axis=1)
    
    return processed, processed_date, target, unique_identifier

def encode_categorical_columns(df):
    """Encode categorical and boolean columns"""
    cat_columns = df.select_dtypes(include=['O']).columns
    num_columns = df.select_dtypes(include=['float64', 'int64', 'int32', 'float32']).columns
    bool_columns = df.select_dtypes(['bool']).columns
    
    for column in list(cat_columns) + list(bool_columns):
        label_encoder = LabelEncoder()
        df[column] = label_encoder.fit_transform(df[column])
    
    return df

def split_data_by_date(data, date_column, split_date):
    """Split dataset based on date"""
    training_data = data[data[date_column] < split_date]
    testing_data = data[data[date_column] >= split_date]
    return training_data, testing_data

def train_and_evaluate_model(model, X_train, X_test, y_train, y_test, model_name=""):
    """Train and evaluate a model"""
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    
    print(f"\nModel: {model_name}")
    print('Accuracy:', accuracy_score(y_test, predictions))
    print('F1 Score:', f1_score(y_test, predictions))
    print('Classification Report:\n', classification_report(y_test, predictions))
    print('Confusion Matrix:\n', confusion_matrix(y_test, predictions))
    
    return model, predictions

def main():
    # Connect to database
    conn = create_db_connection()
    
    # Load data
    data = load_data(conn, "demographics_table", "mart_schema")
    
    # Prepare data
    processed, processed_date, target, unique_identifier = prepare_data(
        data, 
        id_column='id',
        target_column='target',
        date_column='created'
    )
    
    # Encode categorical columns
    processed = encode_categorical_columns(processed)
    
    # Split data by date
    training_data, testing_data = split_data_by_date(
        pd.concat([processed_date, processed], axis=1),
        'created',
        split_date='2022-07-01'
    )
    
    # Prepare features and target for modeling
    X = training_data.drop(['target', 'created'], axis=1)
    y = training_data['target']
    
    # Create train and validation sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.33, random_state=42
    )
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Define models to evaluate
    models = {
        'GaussianNB': GaussianNB(),
        'LGBMClassifier': LGBMClassifier(random_state=42, is_unbalance=True),
        'CatBoostClassifier': CatBoostClassifier(random_state=42, verbose=False),
        'XGBClassifier': XGBClassifier(random_state=42)
    }
    
    # Train and evaluate models
    best_model = None
    best_score = 0
    
    for name, model in models.items():
        model, predictions = train_and_evaluate_model(
            model, X_train_scaled, X_test_scaled, y_train, y_test, name
        )
        
        # Track best model
        score = f1_score(y_test, predictions)
        if score > best_score:
            best_score = score
            best_model = model
    
    # Save best model
    if best_model is not None:
        model_filename = f'models/best_model_{best_score:.2f}.pkl'
        pickle.dump(best_model, open(model_filename, 'wb'))
        print(f"\nBest model saved to {model_filename}")

if __name__ == "__main__":
    main()