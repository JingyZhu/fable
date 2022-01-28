import json
import requests
from bs4 import BeautifulSoup
import os
import time

from azure.storage.queue import (
        QueueClient,
        TextBase64EncodePolicy,
)


current_page = "https://en.wikipedia.org/w/index.php?title=Category:Articles_with_permanently_dead_external_links"
QUEUE_URL = "https://fablestorage.queue.core.windows.net/client"
SAS_KEY = os.getenv("SAS_KEY")

def addToQueue(url):
    requestObj = {
        "email": "",
        "url": "https://en.wikipedia.org" + str(url),
    }

    queue = QueueClient.from_queue_url(
        QUEUE_URL,
        credential=SAS_KEY,
        message_encode_policy=TextBase64EncodePolicy(),
    )

    jsonString = json.dumps(requestObj)

    queue.send_message(jsonString)
    

def main():
    global current_page
    page = requests.get(current_page)
    parsedPage = BeautifulSoup(page.content, "html.parser")
        
    sections = parsedPage.find_all("div", {"class": ["mw-category-group"]})

    for section in sections:
        links = section.find_all("li")

        for link in links:
            tag = link.find("a")
            addToQueue(tag["href"])
            time.sleep(300)
    

    # Find the Next Link
    nextLinkTags = parsedPage.find_all("a", {"title": ["Category:Articles with permanently dead external links"]})
    nextPage = ""

    for nextLink in nextLinkTags:
        if nextLink.string == "next page":
            nextPage = nextLink["href"]
            break
    
    if nextPage:
        current_page = "https://en.wikipedia.org" + nextPage
    else:
        current_page = ""

while current_page:
    print("On Current Page" + current_page)
    main()

