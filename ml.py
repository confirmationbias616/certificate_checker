import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import KFold
from sklearn.metrics import f1_score
import pickle


def save_model(model):
    with open("./rf_model.pkl", "wb") as output:
        pickle.dump(model, output)

def load_model():
    with open("/Users/Alex/Coding/certificate_checker/rf_model.pkl", "rb") as input_file:
        return pickle.load(input_file)

def train_model():
    df = pd.read_csv('~/Coding/certificate_checker/data/train_set.csv')
    X = df[[x for x in df.columns if x.endswith('_score')]]# and x not in ['street_number_pr_score', 'city_score', 'owner_score', 'city_pr_score', 'title_length']]]# and x != 'total_score']]
    y = df[['ground_truth']]
    clf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    sm = SMOTE(random_state=42, ratio = 1)
    kf = KFold(n_splits=3, shuffle=True, random_state=41)
    rc_cum, pr_cum, f1_cum = [], [], []
    for train_index, test_index in kf.split(X):
        X_train, X_test = X.values[train_index], X.values[test_index]
        y_train, y_test = y.values[train_index], y.values[test_index]
        X_train_smo, y_train_smo = sm.fit_sample(X_train, y_train)
        clf.fit(X_train_smo, y_train_smo)
        pred = clf.predict(X_test)
        prob = clf.predict_proba(X_test)
        y_test = y_test.reshape(y_test.shape[0],) # shitty little workaround required due to pandas -> numpy  conversion
        results = pd.DataFrame({'truth':y_test, 'total_score':X_test[:,-1], 'prob':prob[:,1], 'pred':pred})
        rc = len(results[(results.truth==1)&(results.pred==1)]) / len(results[results.truth==1])
        pr = len(results[(results.truth==1)&(results.pred==1)]) / len(results[results.pred==1])
        f1 = f1_score(y_test, pred)
        print(results[(results.truth==1)|(results.pred==1)|(results.total_score>0.6)|(results.prob>0.3)].sort_values(['total_score'], ascending=False))
        print(f'number of truthes to learn from: {len([x for x in y_train if x==1])} out of {len(y_train)}')
        print(f'number of tests: {len(results[results.truth==1])}')
        print(pd.DataFrame({'feat':X.columns, 'imp':clf.feature_importances_}).sort_values('imp', ascending=False))
        print(f'recall: {rc}')
        print(f'precision: {pr}')
        print(f'f1 score: {f1}\n\n')
        rc_cum.append(rc)
        pr_cum.append(pr)
        f1_cum.append(f1)
    print(f'average recall: {sum(rc_cum)/len(rc_cum)}')
    print(f'average precision: {sum(pr_cum)/len(pr_cum)}')
    print(f'avergae f1 score: {sum(f1_cum)/len(f1_cum)}')
    X_smo, y_smo = sm.fit_sample(X, y)
    clf.fit(X_smo, y_smo)
    save_model(clf)
    return rc_cum, pr_cum, f1_cum

if __name__ == '__main__':
    train_model()
