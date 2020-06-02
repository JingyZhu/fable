"""
Search broken pages' content
"""
import requests
from urllib.parse import urlparse 
from pymongo import MongoClient
import pymongo

import sys
sys.path.append('../')
import config
from utils import search


