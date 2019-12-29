"""
    Counts the data of different urls status and correcsponds hosts
"""
from pymongo import MongoClient
import pymongo
import sys

sys.path.append('../')
import config
from utils import plot

