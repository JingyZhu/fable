import random
import json

# year = [2004, 2009, 2014, 2018]

# output = {y: [] for y in year}

# for y in year:
#     links = open('logs/link_analysis_new_{}/urls_2xx_status'.format(y), 'r').read().split('\n')
#     while links[-1] == '':
#         del links[-1]
#     links = random.sample(links, 20)
#     output[y] = links

# json.dump(output, open('staus_200.json', 'w+'))

from bs4 import BeautifulSoup
html = """<h2 class="n">
  <span>
    <img src="http://web.archive.org/web/20041117143155im_/http://ly.lygo.com/ly/qt/logo_aponline.gif" alt="AP Online story">
  </span>
  <br class="none">Trial Shows How Spammers Operate</h2>"""

soup = BeautifulSoup(html, 'html.parser')
tag = soup.find('h2')
print(tag.text)