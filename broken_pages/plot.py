from matplotlib import pyplot as plt
import sys
from pymongo import MongoClient
import re
import counts

sys.path.append('../')
from utils import plot
import config 

db = MongoClient(config.MONGO_HOSTNAME).web_decay

def frac_45xx_links():
    years = [1999, 2004, 2009, 2014, 2019]
    missing_counts = []
    for year in years:
        print(year)
        missing_count = []
        hosts = db.url_status_implicit_broken.aggregate([
            {"$match": {"year": year}},
            {"$group": {"_id": "$hostname"}}
        ])
        for host in hosts:
            count = db.url_status_implicit_broken.count_documents({"year": year, "hostname": host['_id'], "status": re.compile('^[45]')})
            missing_count.append(count)
        missing_counts.append(missing_count)
    plot.plot_CDF(missing_counts, classname=[str(y) for y in years], show=False)
    plt.xlabel("#urls has 4/5xx")
    plt.ylabel("CDF across hosts")
    plt.title("#urls has 4/5xx status code (all hosts)")
    plt.savefig('fig/45xx_frac_all.png')
    plt.close()
    missing_counts = [list(filter(lambda x: x > 0, mc)) for mc in missing_counts]
    plot.plot_CDF(missing_counts, classname=[str(y) for y in years], show=False)
    plt.xlabel("#urls has 4/5xx")
    plt.ylabel("CDF across hosts")
    plt.title("#urls has 4/5xx status code (only 45xx)")
    plt.savefig('fig/45xx_frac.png')
    plt.close()


def frac_DNS_links():
    years = [1999, 2004, 2009, 2014, 2019]
    missing_counts = []
    for year in years:
        print(year)
        missing_count = []
        hosts = db.url_status_implicit_broken.aggregate([
            {"$match": {"year": year}},
            {"$group": {"_id": "$hostname"}}
        ])
        for host in hosts:
            count = db.url_status_implicit_broken.count_documents({"year": year, "hostname": host['_id'], "status": re.compile('DNSError')})
            missing_count.append(count)
        missing_counts.append(missing_count)
    plot.plot_CDF(missing_counts, classname=[str(y) for y in years], show=False)
    plt.xlabel("#urls has DNSError")
    plt.ylabel("CDF across hosts")
    plt.title("#urls has DNSError (all hosts)")
    plt.savefig('fig/dns_frac_all.png')
    plt.close()
    missing_counts = [list(filter(lambda x: x > 0, mc)) for mc in missing_counts]
    plot.plot_CDF(missing_counts, classname=[str(y) for y in years], show=False)
    plt.xlabel("#urls has DNSError")
    plt.ylabel("CDF across hosts")
    plt.title("#urls has DNSError (only DNS)")
    plt.savefig('fig/dns_frac.png')
    plt.close()


def frac_broken_links():
    years = [1999, 2004, 2009, 2014, 2019]
    missing_counts = []
    for year in years:
        print(year)
        missing_count = []
        hosts = db.url_status_implicit_broken.aggregate([
            {"$match": {"year": year}},
            {"$group": {"_id": "$hostname"}}
        ])
        for host in hosts:
            count = db.url_status_implicit_broken.count_documents({"year": year, "hostname": host['_id'], "status": re.compile('^[23]')})
            missing_count.append(100 - count)
        missing_counts.append(missing_count)
    plot.plot_CDF(missing_counts, classname=[str(y) for y in years], show=False)
    plt.xlabel("#urls Brokenr")
    plt.ylabel("CDF across hosts")
    plt.title("#urls Broken (all hosts)")
    plt.savefig('fig/broken_frac_all.png')
    plt.close()
    missing_counts = [list(filter(lambda x: x > 0, mc)) for mc in missing_counts]
    plot.plot_CDF(missing_counts, classname=[str(y) for y in years], show=False)
    plt.xlabel("#urls Brokenr")
    plt.ylabel("CDF across hosts")
    plt.title("#urls Broken (only broken)")
    plt.savefig('fig/broken_frac.png')
    plt.close()


def relateion_x_y_links(x, y, xlabel, ylabel):
    """
    x, y: regular expression to match the query
    Plot the relation between #x vs. #DNSError links
    """
    years = [1999, 2004, 2009, 2014, 2019]
    x_counts, y_counts = [], []
    for year in years:
        print(year)
        x_count, y_count = [], []
        hosts = db.url_status_implicit_broken.aggregate([
            {"$match": {"year": year}},
            {"$group": {"_id": "$hostname"}}
        ])
        for host in hosts:
            count = db.url_status_implicit_broken.count_documents({"year": year, "hostname": host['_id'], "status": x})
            x_count.append(count)
            count = db.url_status_implicit_broken.count_documents({"year": year, "hostname": host['_id'], "status": y})
            y_count.append(count)
        x_counts.append(x_count)
        y_counts.append(y_count)
    plt.rc('font', size=20) 
    plot.plot_Scatter(x_counts, y_counts, nrows=5, ncols=1, xlabel=xlabel, ylabel=ylabel, \
                    title='{} vs. {}'.format(xlabel, ylabel), classname=[str(y) for y in years], show=False)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.savefig('fig/{}_{}_relation.png'.format(xlabel, ylabel))
    

def frac_200_broken_links():
    """
    Plot 3 stack barplots of fraction of good, broken, unsure
    in terms of no redir, homepage, non-homepage
    """
    years = [1999, 2004, 2009, 2014, 2019]
    total_data = [[[] for _ in range(3)] for _ in range(3)]
    no_redir, homepage, nonhome = total_data
    for y in years:
        counts.year = y
        data = counts.status_200_broken_frac_link()
        for i in range(3):
            for j in range(3):
                total_data[i][j].append(data[i][j])
    years = [str(y) for y in years]
    plot.plot_stacked_bargroup(no_redir, xname=years, stackname=['Broken', "Unsure", "Good"], show=False)
    plt.ylabel("Fraction")
    plt.title("Breakdown for no redirection urls")
    plt.savefig('fig/noredir_links.png')
    plt.close()
    plot.plot_stacked_bargroup(homepage, xname=years, stackname=['Broken', "Unsure", "Good"], show=False)
    plt.ylabel("Fraction")
    plt.title("Breakdown for homepage redirection urls")
    plt.savefig('fig/homepage_links.png')
    plt.close()
    plot.plot_stacked_bargroup(nonhome, xname=years, stackname=['Broken', "Unsure", "Good"], show=False)
    plt.ylabel("Fraction")
    plt.title("Breakdown for non-homepage redirection urls")
    plt.savefig('fig/nonhome_links.png')
    plt.close()

# relateion_x_y_links(re.compile('^[23]'), re.compile("DNSError"), "2xx", "DNSError")
# relateion_x_y_links(re.compile('^[23]'), re.compile("^[45]"), "2xx", "45xx")
# relateion_x_y_links(re.compile('^[45]'), re.compile("DNSError"), "45xx", "DNSError")

frac_broken_links()