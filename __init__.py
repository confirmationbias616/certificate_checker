import os

for filename in ["cert_db.sqlite3", "rf_model.pkl", "rf_features.pkl"]:
    try:
        os.rename("temp_" + filename, filename)
    except:
        pass
