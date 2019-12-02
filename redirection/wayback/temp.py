import json
import csv
import random

sheet1 = list(csv.reader(open('comp1_Manual_with_similarity.csv', 'r')))
sheet2 = list(csv.reader(open('../data/wayback_url_manual_simi.csv', 'r')))
sheet2 = {k[0]: k[3] for k in sheet2}

new_sheet = []
for line in sheet1:
    line[3] = sheet2[line[0]]
    new_sheet.append(line)

writer = csv.writer(open('comp1_Manual_with_similarity.csv', 'w'))
writer.writerows(new_sheet)
# sheet_dict = {k[0]: k[1] for k in sheet}

# popu = json.load(open('../data/comp1.json', 'r'))
# sample = random.sample(popu.keys(), 30)

# leafpages = json.load(open('../test/wayback_sample_leafpage.json', 'r'))
# htmls = json.load(open('wayback_html_sample.json', 'r'))

# out = []
# sample = list(filter(lambda x: x not in sheet_dict and x in leafpages and leafpages[x] and x in htmls, sample))

# for k in sample:
#     print(k, popu[k]['wayback_url'])
# string_idx = {"Missing": 0, "Not sure": 1, "Same": 2}
# out = {}
# for line in sheet:
#     url = line[0]
#     out[url] = {
#         "wayback_url": line[1],
#         "match": string_idx[line[2]]
#     }

# json.dump(out, open('wayback_url_manual.json', 'w'))