# DB hostname to connect
"""
config for global variable in this project
"""
import yaml
import os

MONGO_HOSTNAME='redwings.eecs.umich.edu'

LOCALSERVER_PORT=24680


if not os.path.exists(os.path.join(os.path.dirname(__file__), 'config.yml')):
    print("No config yaml file find")
else:
    config_yml = yaml.load(open(os.path.join(os.path.dirname(__file__), 'config.yml'), 'r'), Loader=yaml.FullLoader)
    PROXIES = [{'http': ip, 'https': ip } for ip in \
                config_yml.get('proxies')]
    PROXIES = PROXIES + [{}]  # One host do not have to use proxy
    HOSTS = config_yml.get('hosts')
    TMPPATH = config_yml.get('tmp_path')
    SEARCH_CX = config_yml.get('search_cx')
    SEARCH_KEY = config_yml.get('search_key')