import os
import pickle
import pandas as pd
import pandas_gbq
from xgboost import XGBClassifier
from google.oauth2 import service_account
from google.cloud import storage


def readTrainData():
    sql = """
    SELECT * EXCEPT(user_id) FROM `cloud-714.mack_churn_views.TrainView`
"""
    return pandas_gbq.read_gbq(sql,project_id='cloud-714')

def separateLabels(df):
    labels = df['is_churn']
    df = df.drop(columns=['is_churn'])
    return df, labels

def train(df, labels):
    model = XGBClassifier(eta=0.05,eval_metric='logloss',scale_pos_weight=11)
    model.fit(df, labels)
    pickle.dump(model, open("model.pkl", "wb"))
    return 'model.pkl'

def saveModel(model_file):
    model_directory = os.environ['AIP_MODEL_DIR']
    storage_path = os.path.join(model_directory, model_file)
    blob = storage.blob.Blob.from_string(storage_path, client=storage.Client())
    blob.upload_from_filename(model_file)

if __name__ == '__main__':
    df = readTrainData()
    df, labels = separateLabels(df)
    model_file = train(df, labels)
    saveModel(model_file)
