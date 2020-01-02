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


def RandomSample(collection, sample_size):
    """
    Random sampling some obj from db
    """
    return list(collection.aggregate([{'$sample': {'size': sample_size}}, {'$project': {'_id': False}}]))


def Hosts_gte_N_links_in_year(db, N, year):
    """
    Query hosts with more than N added urls on wayback Machine on certain year
    Return iterator
    """
    cursor = db.hosts_added_links.aggregate([
        {"$match": {"year":year, "added_links": {"$gte": N}}},
        {"$lookup": {
            "from": "hosts_meta",
            "let": {"hostname": "$hostname", "year": "$year"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$hostname", "$$hostname"]},
                    {"$eq": ["$year", "$$year"]}
                ]}}}
            ],
            "as": "meta"
        }},
        {"$match": {"meta.0": {"$exists": True}}},
        {"$project": {"meta": False}}
    ])
    return cursor