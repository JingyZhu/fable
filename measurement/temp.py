from bs4 import BeautifulSoup
from dateutil import parser as dparser
import sys

sys.path.append('../')
from utils.

html = open('temp.html', 'r').read()
soup = BeautifulSoup(html, 'html.parser')
text = soup.get_text(separator=' ')
dt, fuzzy_token = dparser.parse(text, fuzzy_with_tokens=True)


print(dt.strftime("%Y %m %d"))