"""
Global tracer for recording the metadata gathered during finding aliases
"""
import pymongo
from pymongo import MongoClient
import brotli
import re, os
from collections import defaultdict
from urllib.parse import urlsplit, urlparse
import logging
import inspect

from . import config
from .utils import url_utils

db = config.DB

default_name = 'fable'

class tracer(logging.Logger):
    def __init__(self, name=default_name, db=db):
        """
        name: name of the trace
        update_data: {url: update data} for updating the database
        """
        logging.Logger.__init__(self, name)
        self.name = name
        self.db = db
        self._init_logger()
        self.update_data = defaultdict(dict)
    
    def _init_logger(self):
        """
        Init logger data structure
        """
        self.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
        file_handler = logging.FileHandler(self.name + '.log')
        file_handler.setFormatter(formatter)
        std_handler = logging.StreamHandler()
        std_handler.setFormatter(formatter)

        self.addHandler(file_handler)
        self.addHandler(std_handler)
        # return logger
    
    def _get_stackinfo(self, level=2):
        """level: relative stack pos to current stack"""
        st = inspect.stack()[level]
        return st.filename, st.function, st.lineno

    def wayback_url(self, url, wayback):
        self.update_data[url]['wayback_url'] = wayback
        filename, func, lineno = self._get_stackinfo()
        self.info(f'[{filename} {func}:{lineno}] \n Wayback: {wayback}')
    
    def title(self, url, title, titlewosuffix=None):
        self.update_data[url]['title'] = title
        if titlewosuffix:
            self.update_data[url]['title'] = titlewosuffix
        filename, func, lineno = self._get_stackinfo()
        self.info(f'[{filename} {func}:{lineno}] \n title: {title}')
    
    def topN(self, url, topN):
        self.update_data[url]['topN'] = topN
        filename, func, lineno = self._get_stackinfo()
        self.info(f'[{filename} {func}:{lineno}] \n topN: {topN}')
    
    def search_results(self, url, engine, typee, results):
        """
        type: topN/title_site/title_exact
        engine: google/bing
        """
        if f"search_{typee}" not in self.update_data[url]:
            self.update_data[url][f"search_{typee}"] = {'google': [], 'bing':[]}
        self.update_data[url][f"search_{typee}"][engine] = results
        filename, func, lineno = self._get_stackinfo()
        self.info(f'[{filename} {func}:{lineno}] \n search results {typee} {engine}: \n {results}')
    
    def flush(self):
        self.info(f'Flushing URL(s)')
        for url, d in self.update_data.items():
            try:
                self.db.traces.update_one({'url': url}, {'$set': {self.name: d}}, upsert=True)
            except Exception as e:
                self.warn(f'flush exception {url}: {str(e)}')
        

gtracer = None

def set_gtracer(name=default_name, db=db):
    """set a global tracer under this file for different file to call"""
    gtracer = tracer(name, db)

def clear_gtracer():
    gtracer = None