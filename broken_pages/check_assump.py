"""
Script used to check the assumptions
"""
import sys
from pymongo import MongoClient
import json
import os
import queue, threading
import brotli
import socket
import random
import re

sys.path.append('../')
from utils import text_utils, crawl, url_utils, plot
import config

idx = config.HOSTS.index(socket.gethostname())
proxy = config.PROXIES[idx]
db = MongoClient(config.MONGO_HOSTNAME).web_decay
counter = 0


