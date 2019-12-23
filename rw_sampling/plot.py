import json
from pymongo import MongoClient
import sys
import matplotlib.pyplot as plt
import matplotlib
import inspect

sys.path.append('../')
from utils import plot

def plot_diff_union():
    data = json.load(open('links_diff.json', 'r'))
    union_diff = [[], []]
    for obj in data.values():
        union_diff[0].append(int(obj['Union']))
        union_diff[1].append(int(obj['Diff']))
    plot.plot_CDF(union_diff, classname=['union', 'diff'], show=False, cut=0.99)
    plt.xlabel('#Hosts')
    plt.ylabel('CDF 99%')
    plt.title('#Different hosts for chrome load and requests load')
    plt.show()


def plot_add_links():
    more = [{}, {}, {}, {}] #more than 10, 100, 1k and 10k
    exact = []
    appears = [{}, {}, {}, {}]
    for obj in db.added_links.find():
        hostname, year, added_links = obj['hostname'], int(obj['year']), int(obj['added_links'])
        end_idx = int(math.log10(added_links)) if int(math.log10(added_links)) <= 4 else 4
        for i in range(0, end_idx):
            more[i].setdefault(year, set())
            more[i][year].add(hostname)
            appears[i].setdefault(hostname, 0)
            appears[i][hostname] += 1
        exact.append(added_links)
    years = []
    for i in range(4):
        years += list(more[i].keys())
    years = sorted(list(set(years)))
    data = [[] for _ in range(4)]
    for year in years:
        for i in range(4):
            num_links = len(more[i][year]) if year in more[i] else 0
            data[i].append(num_links)
    plot.plot_bargroup(data, years, ['10', '100', '1k', '10k'], show=False)
    plt.ylabel('#hosts')
    plt.title('#Hosts with more than #added links in certain year')
    plt.show()
    # Plot CDF of #links across host*yuear
    plot.plot_CDF([exact], show=False)
    plt.xscale('log')
    plt.title('{Year, Host} #added links')
    plt.xlabel('#added links')
    plt.ylabel('CDF {year, host}')
    plt.show()
    # Plot overlap years
    appears = [list(a.values()) for a in appears]
    plot.plot_CDF(appears, classname=['10', '100', '1k', '10k'], show=False)
    plt.title('#years each hosts appears in each category')
    plt.ylabel('CDF of hosts')
    plt.xlabel('years')
    plt.show()


def plot_latest_year():
    db = MongoClient().wd_test
    data = []
    for obj in db.latest_year.find():
        year = int(obj['year'])
        data.append(year)
    plot.plot_CDF([data], classname=['year'], show=False)
    plt.xlabel('Last year of snapsot')
    plt.ylabel('CDF across hosts')
    plt.title('Last year of snapshots for each hosts')
    plt.show()


def plot_chrome_request_diff():
    db = MongoClient().web_decay
    data = [[], [], []]
    for obj in db.chrome_request_diff.find():
        c_r = len(obj['c-r'])
        r_c = len(obj['r-c'])
        union = len(obj['union'])
        data[0].append(c_r)
        data[1].append(r_c)
        data[2].append(union)
    plot.plot_CDF(data, classname=['C-R', 'R-C', 'Union'], cut=0.99, show=False)
    plt.xlabel('#Host for outgoing links')
    plt.ylabel('CDF across links')
    plt.title("Chrome requests load #hosts difference")
    plt.show()


def plot_rw_stats():
    fontsize = 20
    font = {'size'   : 16}
    matplotlib.rc('font', **font)
    years = [1999, 2004, 2009, 2014, 2019]
    depth = [[] for _ in years]
    collected_hosts = [[] for _ in years]
    for i, year in enumerate(years):
        data = json.load(open('rw_stats/rw_stats_{}.json'.format(year), 'r'))
        for d, ch in data:
            depth[i].append(d)
            collected_hosts[i].append(ch)
    plot.plot_CDF(depth, classname=years, show=False)
    plt.xlabel('Depth', fontsize=fontsize)
    plt.ylabel('CDF across walks', fontsize=fontsize)
    plt.title('Depth of each Random Walk', fontsize=fontsize)
    plt.show()
    plot.plot_CDF(collected_hosts, classname=years, show=False, cut=0.99)
    plt.xlabel('Collected (foreign) hosts', fontsize=fontsize)
    plt.ylabel('CDF across walks', fontsize=fontsize)
    plt.title('Collected (foreign) hosts of each Random Walk', fontsize=fontsize)
    plt.show()


if __name__ == '__main__':
    default_func = 'plot_diff_union'
    calling_dict = {name: var for name, var in locals().items() if inspect.isfunction(var)}
    func = default_func if len(sys.argv) < 2 else sys.argv[1]
    calling_dict[func]()
