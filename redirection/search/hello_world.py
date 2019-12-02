import requests
import json
# from urllib.parse

query = "+\"Feds tell states 'VoIP is ours'\""

google_query_dict = {
    "q": query,
    "key" : "AIzaSyCq145QuNtIRGsluXo4n6lUbrvdOLA_hCY",
    "cx" : "006035121867626378213:tutaxpqyro8",
}

headers = {"Ocp-Apim-Subscription-Key": '978290f3b37c48538596753b4d2be65f'}

bing_query_dict = {
    "q": query
}

google_url = 'https://www.googleapis.com/customsearch/v1'
bing_url = 'https://api.cognitive.microsoft.com/bing/v7.0/search'


def google_extract(r):
    return [v['link'] for v in r['items']]

def bing_extract(r):
    print(r)
    return [v['url'] for v in r['webPages']['value']]

r = requests.get(bing_url, params=bing_query_dict, headers=headers)
r = r.json()

r = bing_extract(r)

f = open('sample.json', 'w+')
json.dump(r, f)
