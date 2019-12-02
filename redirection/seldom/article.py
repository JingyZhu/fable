"""
Get all the article given post id
"""
import requests
import os
import json
import queue
import threading
from subprocess import call

NUM_THREADS = 30
id_queue = queue.Queue()
lock = threading.Lock()
count = 0
BASEURL = 'html'


def init_queue():
    blog_id_file = open('blog_id.json', 'r')
    id_list = json.load(blog_id_file)
    # id_list = id_list[:100]
    for id_li in id_list:
        for idd in id_li[1]:
            id_queue.put(idd)


def generate_subject(subject):
    if subject is None:
        return "no subject"
    subject = subject.replace('/', '-')
    if len(subject) > 200:
        return subject[:200] + 'too long'
    return subject


def init_dir():
    for i in range(2004, 2020):
        call(['mkdir', '-p', os.path.join(BASEURL, str(i))])


def thread_func():
    global count, lock
    f = open('req_post.json', 'r')
    post_json = json.load(f)
    while not id_queue.empty():
        post_id = id_queue.get()
        post_json["methodCalls"][0][1]["ids"] = [post_id]
        r = requests.post('https://ip.topicbox.com/jmap', json=post_json)
        article_prop = r.json()["methodResponses"][0][1]["list"][0]
        article = list(article_prop["bodyValues"].values())[0]["value"]
        sentAt = article_prop["sentAt"]
        subject = article_prop["subject"]
        subject = generate_subject(subject)
        subject += ' ' + post_id
        year = sentAt.split('-')[0]
        html = open(os.path.join(BASEURL, year, subject ) + '.html', 'w+')
        html.write(article)
        html.close()
        lock.acquire()
        print("{}: {} {}".format(count, year, subject))
        count += 1
        lock.release()
        id_queue.task_done()


def main():
    init_queue()
    init_dir()
    threads = []
    for i in range(NUM_THREADS):
        threads.append(threading.Thread(target=thread_func))
        threads[i].start()
    id_queue.join()
    

if __name__ == '__main__':
    main()
