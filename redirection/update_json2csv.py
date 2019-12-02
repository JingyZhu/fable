"""
Put JSON into csv format
Note: JSON could only be key: {key: value pair}
"""
import json
import csv

INPUT_FILE = 'data/comp1.json'
OUTPUT_FILE = 'data/comp1.csv'


data = json.load(open(INPUT_FILE, 'r'))
field = ['url'] + list(data[list(data.keys())[0]].keys())
csv_writer = csv.DictWriter(open(OUTPUT_FILE, 'w+'), fieldnames=field)
csv_writer.writeheader()

for k, v in data.items():
    v['url'] = k
    csv_writer.writerow(v)