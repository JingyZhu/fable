# DB hostname to connect
import yaml
import os

MONGO_HOSTNAME='localhost'

if not os.path.exists('config.yml'):
    print("No config yaml file find")
else:
    PROXIES = [{'http': ip, 'https': ip } for ip in \
        yaml.load(open('config.yml', 'r'), Loader=yaml.FullLoader)['proxies']]
    HOSTS = yaml.load(open('config.yml', 'r'), Loader=yaml.FullLoader)['hosts']