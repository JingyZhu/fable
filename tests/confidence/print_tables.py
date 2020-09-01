import json
import pymongo
from pymongo import MongoClient
from urllib.parse import urlsplit
import os
from collections import defaultdict
import time
import pandas as pd

import sys
sys.path.append('../../')
from ReorgPageFinder import confidence

cl = pd.read_csv('confidence_label.csv', keep_default_na=False)
cl = cl.to_dict(orient='records')

confidence.features_2_table(cl)