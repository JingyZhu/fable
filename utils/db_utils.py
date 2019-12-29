from pymongo import MongoClient

db1 = 'web_decay_test'
db2 = 'web_decay_test'
collections = ['redirection', 'html', 'search', 'search_html']

def CopyCollections(database_src, collection_src, database_dst, collection_dst):
    """
    Copy collections from one db to another with the same collection name
    """
    db_src = MongoClient()[database_src][collection_src]
    db_dst = MongoClient()[database_dst][collection_dst]
    for obj in db_src.find():
        db_dst.insert_one(obj)


def RandomSample(db, sample_size):
    """
    Random sampling some obj from db
    """
    return list(db.aggregate([{'$sample': {'size': sample_size}}, {'$project': {'_id': False}}]))
