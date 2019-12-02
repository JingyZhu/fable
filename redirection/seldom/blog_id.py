"""
Get all the ids on https://ip.topicbox.com/groups/ip
"""
import requests
import threading
import json

id_set = {}
number_id = {}
NUM_THREAD = 16

def thread_func(start, end):
    global number_id, id_set
    id_set = {}
    f = open('req.json', 'rb')
    post_json = json.load(f)
    i = start
    while i < end:
        post_json["methodCalls"][0][1]["position"] = i
        r = requests.post('https://ip.topicbox.com/jmap', json=post_json)
        json_list = r.json()["methodResponses"]
        ids = json_list[0][1]['ids']
        for idd in ids:
            id_set[idd] = 'dummy'
        print("Blogs {} - {} got!".format(i, i + 10))
        number_id[i] = ids
        i += 10


def main():
    global number_id
    threads = []
    for i in range(NUM_THREAD):
        threads.append(threading.Thread(target=thread_func, args=(i*2000, (i+1)*2000), ))
        threads[i].start()
    for i in range(NUM_THREAD):
        threads[i].join()
    print(len(number_id))
    id_result = sorted(number_id.items())
    result = open('blog_id.json', 'w+')
    json.dump(id_result, result)
 
if __name__ == '__main__':
    main()