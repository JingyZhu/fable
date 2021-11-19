from azure.storage.queue import (
        QueueClient
)

import json

queueURL = "https://fablestorage.queue.core.windows.net/output"
sasToken = "?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupix&se=2021-12-02T14:42:05Z&st=2021-11-11T06:42:05Z&spr=https&sig=pbMyft6gYJ0FtyciNqMh%2FfSCt%2BmMAfeIVarq4lp1j9I%3D"
queueObj = QueueClient.from_queue_url(
                queueURL, 
                credential=sasToken,
            )

while True:
    if queueObj.get_queue_properties().approximate_message_count > 0:
        try:
            message = queueObj.receive_message()
            queueObj.delete_message(message.id, message.pop_receipt)
            urlInfo = json.loads(message.content)
            print(message)
        except:
            pass
