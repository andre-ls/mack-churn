import os
import pickle
import pandas as pd
import pandas_gbq
from xgboost import XGBClassifier
from google.oauth2 import service_account
from google.cloud import storage


def loadModel():
    # Load model from GCS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('mack-churn-lake')
    blob = bucket.blob('Model/model/model.pkl')
    blob.download_to_filename('/tmp/model.pkl')

    # Load model
    model = pickle.load(open('/tmp/model.pkl', 'rb'))
    return model

def readData():
    # Read data from BigQuery
    query = 'SELECT * FROM `cloud-714.mack_churn_views.PredictView`'
    df = pandas_gbq.read_gbq(query, project_id='cloud-714')
    return df

def predict(df, model):

    # Preprocess
    df = df.set_index('user_id')

    # Predict
    y_pred = model.predict(df)
    df['predictions'] = y_pred

    return df

def writeData(df):
    # Write data to BigQuery
    pandas_gbq.to_gbq(df[['predictions']], 'mack_churn_gold.ChurnPredictions_04_2017', project_id='cloud-714', if_exists='replace')

if __name__ == '__main__':
    model = loadModel()
    df = readData()
    df = predict(df, model)
    writeData(df)
