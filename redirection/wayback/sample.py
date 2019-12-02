"""
Sample links from wayback_urls.json
"""

import json
import os
import random

f = open('wayback_urls.json', 'r')
data = json.load(f)

keys = list(data.keys())
keys = random.sample(keys, 500)

new_data = {k: data[k] for k in keys}

f = open('wayback_urls_sample.json', 'w+')
json.dump(new_data, f)