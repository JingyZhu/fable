import pymongo

client = pymongo.MongoClient('mongodb://fable-database:mSMNajjnkR1R5lGXxXihhJF5DUKvyyEhrWeBUBE0Mr8mqWsCfOhpsi2zp8ihUzWGaZdHaFKD3G5qF1P6ZMQYaw==@fable-database.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@fable-database@')
d = dict((db, [collection for collection in client[db].collection_names()])
            for db in client.database_names())

print(client['fable']['reorg'])
print(d)

cursor = client['fable']['reorg'].find({})
for document in cursor:
        print(document)




