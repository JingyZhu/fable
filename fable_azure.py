""" Run Fable using Azure services """
import logging
import json
import pymongo
import pywikibot
from fable import ReorgPageFinder
from azure_client import AzureClient
from azure.storage.queue import (
        QueueClient,
        TextBase64EncodePolicy,
)

from bson.objectid import ObjectId

rpf = ReorgPageFinder(classname='achitta', logname='achitta', loglevel=logging.DEBUG)
azureClient = AzureClient()


queueURL = "https://fablestorage.queue.core.windows.net/output"
sasToken = "?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupix&se=2021-12-02T14:42:05Z&st=2021-11-11T06:42:05Z&spr=https&sig=pbMyft6gYJ0FtyciNqMh%2FfSCt%2BmMAfeIVarq4lp1j9I%3D"

client = pymongo.MongoClient('mongodb://fable-database:mSMNajjnkR1R5lGXxXihhJF5DUKvyyEhrWeBUBE0Mr8mqWsCfOhpsi2zp8ihUzWGaZdHaFKD3G5qF1P6ZMQYaw==@fable-database.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@fable-database@')
db = client["fable"]

def addToURLCollection(document):
    
    urlCollection = db["bot_urls"]
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

def postFormatter(requestObject):
    content = ""
    
    # Create Base Header
    content += "Broken Link Aliases For {0}\n".format(requestObject["base_url"])

    # Add Link Aliases
    for link, alias in requestObject["broken_links"].items():
        content += "{0} has the alias: {1}\n".format(link, alias)
    
    return content


def postToWiki(requestObject):
    print("Posting to Wiki Page")
    site = pywikibot.Site("test", "wikidata")
    repo = site.data_repository()
    page = pywikibot.Page(site, "User talk:Anishnya123")

    heading = "== Fable Bot Edit =="
    content = postFormatter(requestObject)
    message = "\n\n{}\n{} --~~~~".format(heading, content)

    page.save(summary="Testing", watch=None, minor=False, botflag=True,
                force=False, callback=None,
                apply_cosmetic_changes=None, appendtext=message)

def pkill(pattern):
    try:
        subprocess.run(["pkill", "-f", pattern], check=False)
    except:
        pass

# Run Fable and upload logs on success to Azure files
def fable_api(urlInfo: dict):
    print(urlInfo)
    email = urlInfo["email"]
    base_URL = str(urlInfo["base_url"])
    broken_links = urlInfo["broken_links"]

    articleCollection = db["bot_articles"]

    # Check to see if article exists
    if articleCollection.find_one({"article_url": base_URL }):
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
    print(aliasIDS)

    # Add to DB
    newDoc = {
        "alias_ids": list(map(lambda x: ObjectId(x), aliasIDS)),
        "article_title": "",
        "article_url": base_URL,
        "article_url_title": base_URL.split("/")[-1]
    }

    print(newDoc)

    articleCollection.insert_one(newDoc)

    # # Create a request object
    # requestObject = {
    #     "email": email,
    #     "base_url": baseURL,
    #     "broken_links": broken_link_map,
    # }

    # # postToWiki(requestObject)
    # queue = QueueClient.from_queue_url(
    #             queueURL, 
    #             credential=sasToken,
    #         )
    
    # jsonString = json.dumps(requestObject)
    # queue.send_message(jsonString)


# Read URLs from Azure Queues and run Fable on them    
def main():
    count = 0
    with open('progress.txt', 'a') as progress_file:
        while azureClient.get_queue_length() > 0:
            try:
                # Kill any stale chrome processes in case of memory issues
                pkill('chrome')

                urlInfo = azureClient.poll_message()
                # progress_file.write(f"Processing number {count}\tHostname: {urlInfo['hostname']}\n")
                fable_api(urlInfo)
                count += 1
            except:
                pass

if __name__ == "__main__":
    main()
