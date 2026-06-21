import csv
import json
import random
import time

import pandas as pd
import redis
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
import numpy as np

df = pd.read_csv("./dataset/cloud_dataset.csv")
df.drop(["Timestamp", "User_ID", "Workload_Type"], axis=1, inplace=True)

print(df.head())


X = df.drop(columns=["Anomaly_Label"])
y = df.get("Anomaly_Label")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
FEATURE_COLS = X_train.columns.tolist()

print("X_train:", len(X_train))
print("X_test:", len(X_test))
print("y_train:", len(y_train))
print("y_test:", len(y_test))
clf = XGBClassifier()
clf.fit(X_train, y_train)

r = redis.Redis(
    host="localhost", port=6379, db=0, decode_responses=True
)
try:
    if r.ping():
        print("Connected to Redis!")
except redis.ConnectionError:
    print("Could not connect to Redis.")


def LSTM_START():
    while True:
 
        rows = []
        # lấy tất cả key NODE-*
        keys = r.keys("NODE-*")
        for key in keys:
            try: 
                data = r.execute_command("JSON.GET", key)
                # print(data)
                if data:
                    rows.append(json.loads(data))
        

            except Exception as e:
                 print("Error:", e)
  
        X_test = pd.DataFrame(rows)

        node_ids = X_test.get("NODE_ID")

        X_predict = X_test.drop(columns=["NODE_ID"])
        # Keep feature order consistent with training
        X_predict = X_predict.reindex(columns=FEATURE_COLS).fillna(0)
        print(X_predict.head())


        pred = clf.predict(X_predict)

        anomaly_nodes = [node_ids.iloc[i] for i, p in enumerate(pred) if p == 0]

        # print("Anomaly Nodes:", anomaly_nodes)

        if len(anomaly_nodes) == 0:
            random_index = random.randrange(len(X_test))
            # print("Random Index:", random_index)
            # print("X_test:", X_test[random_index])
            r.set("XGBoost_TOP", str(X_test.iloc[random_index].to_dict()))
        if len(anomaly_nodes) > 0:
            random_index = random.randrange(len(anomaly_nodes))
            # print("anomaly_nodes > 0:", anomaly_nodes[random_index])
            r.set("XGBoost_TOP", str(anomaly_nodes[random_index]))

        time.sleep(5)


# print("accuracy_score:", accuracy_score(y_test, pred))
LSTM_START()
