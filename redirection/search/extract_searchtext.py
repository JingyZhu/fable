"""
Extract search text from html
"""
from bs4 import BeautifulSoup
import json


def get_headers(html):
    soup = BeautifulSoup(html, 'lxml')
    magic_num = 12
    text = ''
    tags = soup.find_all("title")
    for tag in tags:
        if tag.text != "Wayback Machine": 
            text += tag.text + ' '
    if len(text.split()) > magic_num:
        return text
    metadata = soup.find_all('meta', {"name": "keywords"})
    for tag in metadata:
        text += tag['content'] + ' '
    if len(text.split()) > magic_num:
        return text
    metadata = soup.find_all('meta', {"name": "description"})
    for tag in metadata:
        text += tag['content'] + ' '
    if len(text.split()) > magic_num:
        return text
    for i in range(1, 7):
        tags = soup.find_all('h' + str(i))
        for tag in tags:
            text += tag.text + ' '
    return text


def pretty_searchtext(html):
    text = get_headers(html)
    if text is None:
        return text
    replace_list = ['\n', '\t', '-', ',', '|', '>', '<']
    for char in replace_list:
        text = text.replace(char, "")
    if text == "":
        return None
    text = text.split()
    text = text[:32]
    return ' '.join(text)


def main(html_data, meta_data):
    count = 1
    new_meta_data = {}
    for url, html in html_data.items():
        # print(count)
        # count += 1
        st = pretty_searchtext(html)
        if st is not None:
            value = meta_data[url]
            value['searchtext'] = st
            new_meta_data[url] = value

    json.dump(new_meta_data, open('wayback_urls_sample.json', 'w+'))


if __name__ == '__main__':
    html_data = json.load(open('wayback_html_sample.json', 'r'))
    meta_data = json.load(open('wayback_urls_sample.json', 'r'))
    main(html_data, meta_data)