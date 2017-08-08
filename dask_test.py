import numpy as np
import dask.dataframe as dd
import re
from collections import defaultdict
import _pickle
#from cloudpickle import dumps, loads
import cloudpickle

csv_file = 'data/testdata.manual.2009.06.14.csv'
df = dd.read_csv(csv_file, dtype='str')

print(df.head(2))

def process_data(df_data_train, df_data_test):
    def clean_str(string):
        string = re.sub(r"[^A-Za-z0-9(),!?\'\`]", " ", string)     
        string = re.sub(r"\'s", " \'s", string) 
        string = re.sub(r"\'ve", " \'ve", string) 
        string = re.sub(r"n\'t", " n\'t", string) 
        string = re.sub(r"\'re", " \'re", string) 
        string = re.sub(r"\'d", " \'d", string) 
        string = re.sub(r"\'ll", " \'ll", string) 
        string = re.sub(r",", " , ", string) 
        string = re.sub(r"!", " ! ", string) 
        string = re.sub(r"\(", " \( ", string) 
        string = re.sub(r"\)", " \) ", string) 
        string = re.sub(r"\?", " \? ", string) 
        string = re.sub(r"\s{2,}", " ", string)    
        return string.strip().lower()

    df_data_train = df_data_train[["text", "polarity"]]
    df_data_train = df_data_train[df_data_train.polarity != 2]
    df_data_train.text = df_data_train.text.map(lambda x: x.encode('ascii', 'ignore').lower())
    #print(df_data_train.head())
    df_data_test = df_data_test[["text", "polarity"]]
    df_data_test = df_data_test[df_data_test.polarity != 2]
    #print(df_data_test.head())
    df_data_test.text = df_data_test.text.map(lambda x: x.encode('ascii', 'ignore').lower())

    #_test = df_data_train[df_data_train.polarity == 4]
    #print(len(_test))

    _vocab = defaultdict(float) 
    _store = []

    for row in df_data_train.itertuples():
        _text = row.text
        _sentiment = row.polarity

        _text = _text.decode('utf-8')
        _cleaned_text = clean_str(_text.strip()) 
        _words = set(_cleaned_text.split())
        for w in _words:
            _vocab[w] += 1

        _rec = {'y': _sentiment,
                'text': _text,
                'num_words': len(_words),
                'split': int(np.random.rand() < 0.8)}

        _store.append(_rec)

    for row in df_data_test.itertuples():
        _text = row.text
        _sentiment = row.polarity

        _text = _text.decode('utf-8')
        _cleaned_text = clean_str(_text.strip()) 
        _words = set(_cleaned_text.split())
        for w in _words:
            _vocab[w] += 1

        _rec = {'y': _sentiment,
                'text': _text,
                'num_words': len(_words),
                'split': -1}

        _store.append(_rec)


    return _store, _vocab      

csv_test = 'data/testdata.manual.2009.06.14.csv'
csv_train = 'data/training.1600000.processed.noemoticon.csv'
def prepare_data():
    # utf-8
    df_data_train = dd.read_csv(csv_train,
        names=["polarity", "id", "date", "query", "user", "text"],
        encoding='ISO-8859-1', error_bad_lines = False)
    df_data_test = dd.read_csv(csv_test,
        names=["polarity", "id", "date", "query", "user", "text"],
        encoding='ISO-8859-1', error_bad_lines = False)

    _store, _vocab = process_data(df_data_train, df_data_test)

    #
    # decode
    #
    # embedings, unknown words

    #
    # dump
    #
    cloudpickle.dump([_store, _vocab], open('_train_test.cpickle', 'wb'))

import model

def _create_model():

# Training
def train_data()
    # load data
    _data = cloudpickle.load(open("_train_test.cpickle", "rb"))

    # train

    # make model

    # save model as json

def predict_on_data():

def predict_on_sentence():

if __name__ == '__main__':
    prepare_data()
