""" Run Fable using Azure services """
from ftplib import all_errors
import logging
import json
import pymongo
import time
import os
from fable import ReorgPageFinder
from azure_client import AzureClient
from bson.objectid import ObjectId
from sendEmail import sendEmail

rpf = ReorgPageFinder(classname='achitta', logname='achitta', loglevel=logging.DEBUG)
azureClient = AzureClient()

# Mongo DB Info
MONGO_ID = str(os.getenv("MONGO"))
client = pymongo.MongoClient(MONGO_ID)
db = client["fable"]

# Collections Needed
all_link_collection = db["bot_all_links"]
articleCollection = db["bot_articles"]
urlCollection = db["bot_urls"]

def addLinksToDb(article_id, broken_links):
    print("in here")
    for domainName in broken_links:
        hostname = domainName
        urls = broken_links[hostname]

        for url in urls:
            print("Adding url here")
            newDoc = {
                "url": str(url),
                "article_id": article_id,
                "alias_found": True if urlCollection.find_one({"brokenLink": str(url)}) else False,
            }

            print(newDoc)
            all_link_collection.insert_one(newDoc)
            print("added to collection")

def addToURLCollection(document):
    
    exists = urlCollection.find_one({"brokenLink": document["url"]})

    if exists:
        return str(document["_id"])
    
    generalMethod = str(document["achitta"]["by"]["method"]).capitalize()
    specificMethod = str(document["achitta"]["by"]["type"]).capitalize()

    newDoc = {
        "brokenLink": document["url"],
        "fixedLink": document["achitta"]["reorg_url"],
        "method": str(generalMethod + "-" + specificMethod),
        "correct_votes": 0,
        "cant_tell_votes": 0,
        "inaccurate_votes": 0,
        "beenReplaced": False,
    }

    added = urlCollection.insert_one(newDoc)

    return str(added["_id"]) 

def getAliasesFromDB(broken_links):
    alias_ids = []
    
    for domainName in broken_links:
        hostname = domainName
        urls = broken_links[hostname]

        for url in urls:
            # Add to broken_link_map
            cursor = db['reorg'].find({"url": str(url) })

            for document in cursor:
                if 'achitta' in document:
                    if 'reorg_url' in document['achitta']:
                        alias_ids.append(addToURLCollection(document))
    
    return alias_ids

def pkill(pattern):
    try:
        subprocess.run(["pkill", "-f", pattern], check=False)
    except:
        pass

# Run Fable and upload logs on success to Azure files
def fable_api(urlInfo: dict):
    print(urlInfo)
    
    email = urlInfo["email"]
    title = str(urlInfo["article_title"])
    base_URL = str(urlInfo["base_url"])
    broken_links = urlInfo["broken_links"]
    article_title_url = base_URL.split("/")[-1]

    # Check to see if article exists
    if articleCollection.find_one({"article_url": base_URL }):
        sendEmail(email, base_URL, article_title_url)
        return

    for domainName in broken_links:
        hostname = domainName
        urls = broken_links[hostname]

        try:
            rpf.init_site(hostname, urls)
            rpf.search(required_urls=urls)
            rpf.discover(required_urls=urls)
        except:
            pass
    
    
    aliasIDS = getAliasesFromDB(broken_links)

    print("Got Aliases")

    # Add to DB
    newDoc = {
        "alias_ids": list(map(lambda x: ObjectId(x), aliasIDS)),
        "article_title": title,
        "article_url": base_URL,
        "article_url_title": article_title_url
    }

    newArticle = articleCollection.insert_one(newDoc)

    print("Added Article to DB")

    # Update the db with all the links
    addLinksToDb(newArticle["_id"], broken_links)

    print("Added Links")

    # Send the Email that Fable hs completed
    sendEmail(email, base_URL, article_title_url)

    print("Email Sent")


# Read URLs from Azure Queues and run Fable on them    
def main():
    count = 0
    with open('progress.txt', 'a') as progress_file:
        while True:
            if azureClient.get_queue_length() > 0:
                try:
                    # Kill any stale chrome processes in case of memory issues
                    pkill('chrome')

                    urlInfo = azureClient.poll_message()
                    # progress_file.write(f"Processing number {count}\tHostname: {urlInfo['hostname']}\n")
                    fable_api(urlInfo)
                    count += 1
                except:
                    pass
            else:
                time.sleep(60)

if __name__ == "__main__":
    main()
