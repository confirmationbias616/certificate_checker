import pandas as pd
from scraper import scrape
from wrangler import wrangle
from matcher import match


def build_train_set():
    test_df_dilfo = pd.read_csv('./data/test_raw_dilfo_certs.csv')
    test_web_df = scrape(ref=test_df_dilfo)
    test_web_df = wrangle(ref=test_web_df)
    rand_web_df = pd.read_csv('./data/raw_web_certs_2011-01-01_to_2011-04-30.csv')
    rand_web_df = wrangle(ref=rand_web_df)
    for i, test_row_dilfo in test_df_dilfo.iterrows():
        test_row_dilfo = test_row_dilfo.to_frame().transpose()  # .iterows returns a pd.Series for every row so this turns it back into a dataframe to avoid breaking any methods downstream
        test_row_dilfo = wrangle(ref=test_row_dilfo)
        rand_web_df = rand_web_df.sample(n=len(test_df_dilfo), random_state=i)
        close_matches = match(test_row_dilfo, test_web_df,
            min_score_thresh=0, test=True)
        random_matches = match(test_row_dilfo, rand_web_df,
            min_score_thresh=0, test=True)
        matches = close_matches.append(random_matches)
        matches['ground_truth'] = matches.cert_url.apply(
            lambda x: 1 if x == test_row_dilfo.link_to_cert.iloc[0] else 0)
        matches['dilfo_job_number'] = test_row_dilfo.job_number.iloc[0]
        matches['title_length'] = matches.title.apply(len)
        try:
            all_matches = all_matches.append(matches)
        except NameError:
            all_matches = matches

    all_matches.to_csv(f'./data/train_set.csv', index=False)

if __name__=="__main__":
    build_train_set()
