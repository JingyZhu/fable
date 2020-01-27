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
        missing_count = []
        hosts = db.url_status.aggregate([
            {"$match": {"year": year}},
            {"$group": {"_id": "$hostname"}}
        ])
        for host in hosts:
            count = db.url_status.count_documents({"year": year, "hostname": host['_id'], "status": re.compile('^[45]')})
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
        missing_count = []
        hosts = db.url_status.aggregate([
            {"$match": {"year": year}},
            {"$group": {"_id": "$hostname"}}
        ])
        for host in hosts:
            count = db.url_status.count_documents({"year": year, "hostname": host['_id'], "status": re.compile('DNSError')})
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

frac_45xx_links()
frac_DNS_links()