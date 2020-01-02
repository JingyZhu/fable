from pymongo import MongoClient
import sys
import json
import re

sys.path.append('../')
import config

db = MongoClient(config.MONGO_HOSTNAME).web_decay



