from pymongo import MongoClient
import sys
import json
import re

sys.path.append('../')
import config
from utils import db_utils

db = MongoClient(config.MONGO_HOSTNAME).web_decay
year = 2004

# hosts1 = list(db.url_sample.aggregate([{"$group": {"_id": "$hostname"}}]))


