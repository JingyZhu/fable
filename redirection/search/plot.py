import json
import csv
from matplotlib import pyplot as plt
from pymongo import MongoClient

import sys
sys.path.append('../../')
from utils import plot

def plot_query_similarity():
    db = MongoClient().web_decay
    obj = db.search.find({}, {"topN_similarity": 1, "titlematch_similarity": 1})
    obj = list(obj)
    data = [
        filter(lambda x: x is not None, [ v.get('topN_similarity') for v in obj]), 
        filter(lambda x: x is not None, [ v.get('titlematch_similarity') for v in obj])
    ]
    plot_data = [list(l) for l in data]

    plot.scatter_cdf_plot(plot_data, ['topN', 'titlematch'])
    plt.legend()
    plt.ylabel('CDF')
    plt.xlabel('TF-IDF')
    plt.title('Missing Pages search similarity')
    plt.show()


plot_query_similarity()