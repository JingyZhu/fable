import pymongo
import pywikibot

client = pymongo.MongoClient('mongodb://fable-database:mSMNajjnkR1R5lGXxXihhJF5DUKvyyEhrWeBUBE0Mr8mqWsCfOhpsi2zp8ihUzWGaZdHaFKD3G5qF1P6ZMQYaw==@fable-database.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@fable-database@')

for coll in client["fable"].list_collection_names():
    print(coll)


"""
bot_articles
checked
reorg
na_urls
wayback_rep
crawl
wayback_index
searched
bot_urls
traces
corpus
"""

cursor = client['fable']['wayback_index'].find({})
for document in cursor:
        print(document)




