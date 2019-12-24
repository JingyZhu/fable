# DB hostname to connect
"""
config for global variable in this project
"""
import yaml
import os

MONGO_HOSTNAME='redwings.eecs.umich.edu'

if not os.path.exists(os.path.join(os.path.dirname(__file__), 'config.yml')):
    print("No config yaml file find")
else:
    PROXIES = [{'http': ip, 'https': ip } for ip in \
        yaml.load(open(os.path.join(os.path.dirname(__file__), 'config.yml'), 'r'), Loader=yaml.FullLoader)['proxies']]
    PROXIES = PROXIES + [{}]  # One host do not have to use proxy
    HOSTS = yaml.load(open(os.path.join(os.path.dirname(__file__), 'config.yml'), 'r'), Loader=yaml.FullLoader)['hosts']