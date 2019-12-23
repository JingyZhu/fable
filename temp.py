import requests
import threading
url = 'http://mitglied.lycos.de/evilhassurvived/'
url = 'http://www.nytimes.com'

params = {
        'output': 'json',
        'url': url,
        'from': 19700101,
        'to': 20191231,
        'limit': -50
    }

def thread_func():
    while True:
        try:
            r = requests.get('http://web.archive.org/cdx/search/cdx', params=params)
            print(r.status_code)
            r = r.json()
        except:
            print(r.text)
            return
    

pools = []
for _ in range(20):
    pools.append(threading.Thread(target=thread_func))
    pools[-1].start()

for t in pools:
    t.join()