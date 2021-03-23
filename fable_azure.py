from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)
from azure.storage.fileshare import ShareFileClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import logging
from fable import ReorgPageFinder
import base64
import os, uuid
import json
import sys

def azure_kv(vault_name, secret_name):
    # * Details: https://docs.microsoft.com/en-us/azure/key-vault/secrets/quick-create-python?tabs=cmd
    KVUri = f"https://{vault_name}.vault.azure.net"

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    retrieved_secret = client.get_secret(secret_name)
    return retrieved_secret

class AzureClient:
    def __init__(self):
        vault_name = os.getenv('FABLE_CONFIG_VAULTNAME')
        self.connect_str = azure_kv(vault_name, "storage-connection-str").value
        self.queue_name = azure_kv(vault_name, "queue-name").value
        self.fileshare_name = os.getenv("fileshare-name").value
        self.queue_client = QueueClient.from_connection_string(self.connect_str, self.queue_name)

    def get_length(self) -> int:
        """Get queue length"""
        return self.queue_client.get_queue_properties().approximate_message_count

    def poll_message(self) -> dict:
        """Get message from queue."""
        try:
            message = self.queue_client.receive_message()
            self.queue_client.delete_message(message.id, message.pop_receipt)
            urlInfo = json.loads(message.content)
            return urlInfo
        except:
            return None
    
    def upload_file(self, srcPathLocal, dstPathAzure):
        """Upload local file to Azure files"""
        file_client = ShareFileClient.from_connection_string(conn_str=self.connect_str, 
        share_name=self.fileshare_name, file_path=dstPathAzure)
        
        with open(srcPathLocal, "rb") as source_file:
            file_client.upload_file(source_file)
        
        file_client.close()

rpf = ReorgPageFinder(classname='achitta', logname='achitta', loglevel=logging.DEBUG)

# TODO: Write to logs
def fable_api(urlInfo: dict):
    hostname = urlInfo['hostname']
    urls = urlInfo['urls']
    rpf.init_site(hostname, urls)
    rpf.search(required_urls=urls)
    rpf.discover(required_urls=urls)

def main():
    numToProcess = None
    if len(sys.argv) > 1:
        numToProcess = int(sys.argv[1])
    
    azureClient = AzureClient()
    count = 0
    while (numToProcess is None or count < numToProcess) and azureClient.get_length() > 0:
        urlInfo = azureClient.poll_message()
        fable_api(urlInfo)

if __name__ == "__main__":
    main()