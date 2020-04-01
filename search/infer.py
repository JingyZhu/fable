"""
Use searched url to infer potentially more urls not indexed by google
"""
import requests
import sys
from pymongo import MongoClient
import pymongo
import json
import os
import brotli
import socket
import random
import re, time
import itertools, collections

sys.path.append('../')
from utils import url_utils
import config


def generate_inferred_rules():
    