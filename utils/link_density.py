from bs4 import BeautifulSoup

def filter_separator(string):
    separator = [' ', '\n']
    for sep in separator:
        string = string.replace(sep, '')
    return string


def find_link_density(html):
    """
    Find link density of a webpage given html
    """
    soup = BeautifulSoup(html, 'html.parser')
    filter_tags = ['style', 'script']
    for tag in filter_tags:
        for element in soup.findAll(tag):
            element.decompose()
    total_text = filter_separator(soup.get_text())
    total_length = len(total_text)
    atag_length = 0
    for atag in soup.findAll('a'):
        atag_text = filter_separator(atag.get_text())
        atag_length += len(atag_text)

    return atag_length / total_length

