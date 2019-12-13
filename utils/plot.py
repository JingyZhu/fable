"""
Plot utilities
"""
from matplotlib import pyplot as plt
import numpy as np


def plot_CDF(data, classname=[], savefig='', show=True):
    """
    Plot the CDF for different class
    data should be a 2-dimensional list with each row a label
    """
    data = [sorted(datus) for datus in data]
    print(data)
    if len(classname) != len(data):
        classname = [ str(i + 1) for i in range(len(data))]
    for datus, cn in zip(data, classname):
        length = len(datus)
        x = datus
        y = [ (i + 1) / length for i in range(length)]
        plt.plot(x, y, label=cn)
    plt.legend()
    if savefig:
        plt.savefig(savefig)
    elif show:
        plt.show()


def plot_bargroup(data, xname, barname, savefig='', show=True):
    """
    Plot grouped barplot
    data should be a N*K array. N: number of barnanme, K: number of x labels
    xname: name of xaxis categories (put together one)
    barname: name of groups 
    """
    barWidth = 1/ (len(data) + 1)
    # Set position of bar on X axis

    length = len(data[0])
    r1 = np.arange(length)
    r2 = [x + barWidth for x in r1]
    r3 = [x + barWidth for x in r2]
    
    rs = [ list(map( lambda x: x + i * barWidth, list(range(length)))) for i, datus in enumerate(data) ]
    for r, datus, bn in zip(rs, data, barname):
        plt.bar(r, datus, width=barWidth, label=bn)
    
    # Add xticks on the middle of the group bars
    plt.xlabel('years', fontweight='bold')
    plt.xticks([r + barWidth for r in range(length)], xname)
    
    # Create legend & Show graphic
    plt.legend()
    if savefig:
        plt.savefig(savefig)
    elif show:
        plt.show()

