import json
import csv
from matplotlib import pyplot as plt
from pymongo import MongoClient

import sys
sys.path.append('../../')
from utils import plot


match_string = ['Missing', 'Not sure', 'Same']

def plot_cdf_by_category():
    """
    Plot cdf with different colors of "Same", "Not sure", "Missing"
    Save the detailed data into data/wayback_url_manual_simi.csv
    """
    data = json.load(open('../data/comp1.json', 'r'))
    sample = json.load(open('wayback_url_manual.json', 'r'))

    savedata = {}
    plot_data = [[] for i in range(3)]
    for url, value in sample.items():
        cate = int(value['match'])
        similarity = data[url]['similarity']
        plot_data[cate].append(float(similarity))
        savedata[url] = {
            "wayback_url": data[url]['wayback_url'],
            "match": match_string[cate],
            "similarity": similarity
        }


    plot.scatter_cdf_plot(plot_data, match_string)

    plt.legend()
    plt.ylabel("CDF")
    plt.xlabel("TF-IDF Similarity")
    plt.title("Justext")
    plt.show()

    # fieldnames = ["url", "wayback_url", "match", "similarity"]
    # writer = csv.DictWriter(open("data/wayback_url_manual_simi.csv", 'w+'), fieldnames=fieldnames)

    # for url, value in savedata.items():
    #     value["url"] = url
    #     writer.writerow(value)


def plot_cdf_by_category_db():
    data = MongoClient().web_decay.redirection
    sample = json.load(open('wayback_url_manual.json', 'r'))

    savedata = {}
    plot_data = [[] for i in range(3)]
    for url, value in sample.items():
        cate = int(value['match'])
        similarity = data.find_one({'url': url})['similarity']
        plot_data[cate].append(float(similarity))
        savedata[url] = {
            "wayback_url": data.find_one({'url': url})['wayback_url'],
            "match": match_string[cate],
            "similarity": similarity
        }


    plot.scatter_cdf_plot(plot_data, match_string)

    plt.legend()
    plt.ylabel("CDF")
    plt.xlabel("TF-IDF Similarity")
    plt.title("Justext")
    plt.show()

plot_cdf_by_category_db()