import json
import pywikibot
from azure.storage.queue import (
        QueueClient
)

# Basic Queue Set Up
queueURL = "https://fablestorage.queue.core.windows.net/output"
sasToken = "?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupix&se=2021-12-02T14:42:05Z&st=2021-11-11T06:42:05Z&spr=https&sig=pbMyft6gYJ0FtyciNqMh%2FfSCt%2BmMAfeIVarq4lp1j9I%3D"
queueObj = QueueClient.from_queue_url(
                queueURL, 
                credential=sasToken,
            )

# Message Content Formatter
def formatter(message):

    content = """
                <p>Fable-Bot is a project ran by researchers from The University of Michigan.
                Our aim is to find aliases for broken links across Wikipedia.  
                To learn more about the project, or to report that one of the aliases Fable-Bot
                found was incorrect, please visit https://webresearch.eecs.umich.edu/fable/</p>"
              """
    explainString = """
                        <p>This alias was found using the method of {0} based off 
                        its {1} with a confidence of {2}</p>
                    """

    for link, aliasObj in message["broken_links"].items():
        # Alias Found
        if aliasObj == "NONE":
            content += "<p>No alias found for: {0}</p>".format(link)
            continue

        print(type(aliasObj))
        print(aliasObj)
        aliasObj = json.loads(aliasObj.replace("'", '"'))
        content += "<p>"
        if "reorg_url" in aliasObj:
            print(type(aliasObj))
            aliasDetails = aliasObj["by"]
            content += "{0} has an alias {1}\n".format(link, aliasObj["reorg_url"])
            content += explainString.format(
                aliasDetails["method"], 
                aliasDetails["type"],
                aliasDetails["value"],
            )

        content += "</p>"

    return content

def getArticleName():
    return "User:Anishnya123"

# Page Posting Logic
def postToPage(message):
    messageObj = message
    site = pywikibot.Site("test", "wikidata")
    repo = site.data_repository()
    page = pywikibot.Page(site, getArticleName())

    heading = "== Fable-Bot =="
    content = formatter(messageObj)
    message = "\n\n{}\n{} --~~~~".format(heading, content)

    page.save(summary="Testing", watch=None, minor=False, botflag=True,
             force=False, callback=None,
             apply_cosmetic_changes=None, appendtext=message)


# Main Loop to Run
while True:
    if queueObj.get_queue_properties().approximate_message_count > 0:
        message = queueObj.receive_message()
        queueObj.delete_message(message.id, message.pop_receipt)
        urlInfo = json.loads(message.content)
        print(urlInfo)
        print("Posting to page")
        postToPage(urlInfo)
