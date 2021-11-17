""" Run Fable using Azure services """
import logging
import json
import pymongo
from fable import ReorgPageFinder
from azure_client import AzureClient
from azure.storage.queue import (
        QueueClient,
        TextBase64EncodePolicy,
)

rpf = ReorgPageFinder(classname='achitta', logname='achitta', loglevel=logging.DEBUG)
azureClient = AzureClient()


queueURL = "https://fablestorage.queue.core.windows.net/requests"
sasToken = "?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupix&se=2021-12-02T14:42:05Z&st=2021-11-11T06:42:05Z&spr=https&sig=pbMyft6gYJ0FtyciNqMh%2FfSCt%2BmMAfeIVarq4lp1j9I%3D"

def getAliasFromDB(broken_links):
    client = pymongo.MongoClient('mongodb://fable-database:mSMNajjnkR1R5lGXxXihhJF5DUKvyyEhrWeBUBE0Mr8mqWsCfOhpsi2zp8ihUzWGaZdHaFKD3G5qF1P6ZMQYaw==@fable-database.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@fable-database@')
    broken_link_map = {}
    
    for domainName in broken_links:
        hostname = domainName
        urls = broken_links[hostname]

        for url in urls:
            # Add to broken_link_map
            broken_link_map.update({str(url): "NONE"})
            cursor = client['fable']['reorg'].find({"url": str(url) })

            for document in cursor:
                if 'achitta' in document:
                    if 'reorg_url' in document['achitta']:
                        broken_link_map[url] = str(document['achitta']['reorg_url'])
    
    return broken_link_map


def pkill(pattern):
    try:
        subprocess.run(["pkill", "-f", pattern], check=False)
    except:
        pass

# Run Fable and upload logs on success to Azure files
def fable_api(urlInfo: dict):
    email = urlInfo["email"]
    baseURL = urlInfo["base_url"]
    broken_links = urlInfo["broken_links"]

    for domainName in broken_links:
        hostname = domainName
        urls = broken_links[hostname]
        try:
            rpf.init_site(hostname, urls)
            rpf.search(required_urls=urls)
            rpf.discover(required_urls=urls)
        except:
            pass
    
    
    broken_link_map = getAliasFromDB(broken_links)
    print(broken_link_map)

    # Create a request object
    requestObject = {
        "email": email,
        "base_url": baseURL,
        "broken_links": broken_link_map,
    }

    # Send to queue
    queue = QueueClient.from_queue_url(
                    queueURL, 
                    credential=sasToken,
                    message_encode_policy=TextBase64EncodePolicy()
                )
    
    jsonString = json.dumps(requestObject)
    queue.send_message(jsonString)

# Read URLs from Azure Queues and run Fable on them    
def main():
    count = 0
    with open('progress.txt', 'a') as progress_file:
        while azureClient.get_queue_length() > 0:
            try:
                # Kill any stale chrome processes in case of memory issues
                pkill('chrome')

                urlInfo = azureClient.poll_message()
                progress_file.write(f"Processing number {count}\tHostname: {urlInfo['hostname']}\n")
                fable_api(urlInfo)
                count += 1
            except:
                pass

if __name__ == "__main__":
    main()
