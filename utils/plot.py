"""
Plot utilities
"""
from matplotlib import pyplot as plt
import numpy as np


def plot_CDF(data, classname=[], savefig='', show=True, cut=1):
    """
    Plot the CDF for different class
    data should be a 2-dimensional list with each row a label
    cut: Percent of CDF to show 1 means all
    """
    data = [sorted(datus) for datus in data]
    size = [len(datus) for datus in data]
    # data = [datus[: int(s*cut)] for datus, s in zip(data, size)]
    if len(classname) != len(data):
        classname = [ str(i + 1) for i in range(len(data))]
    for datus, cn in zip(data, classname):
        length = int(len(datus) * cut)
        x = datus[: int(length*cut)]
        y = [ (i + 1) / length for i in range(length)][:int(length*cut)]
        plt.plot(x, y, label=cn)
    plt.legend()
    if savefig:
        plt.savefig(savefig)
    elif show:
        plt.show()


def plot_Scatter(data, classname=[], savefig='', show=True, cut=1):
    """
    Plot the CDF for different class
    data should be a 2-dimensional list with each row a label
    cut: Percent of CDF to show 1 means all
    """
    data = [sorted(datus) for datus in data]
    size = [len(datus) for datus in data]
    # data = [datus[: int(s*cut)] for datus, s in zip(data, size)]
    if len(classname) != len(data):
        classname = [ str(i + 1) for i in range(len(data))]
    for datus, cn in zip(data, classname):
        length = int(len(datus) * cut)
        x = datus[: int(length*cut)]
        y = [ (i + 1) / length for i in range(length)][:int(length*cut)]
        plt.scatter(x, y, label=cn)
    plt.legend()
    if savefig:
        plt.savefig(savefig)
    elif show:
        plt.show()


def plot_bargroup(data, xname, barname, savefig='', show=True):
    """
    Plot grouped barplot
    data should be a N*K array. N: number of barnanme, K: number of x labels
    xname: name of xaxis categories (put together one) size K
    barname: name of groups (size N)
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


def plot_stacked_bargroup(data, xname, stackname, savefig='', show=True):
    """
    Plot Stacked grouped barplot
    data should be a S*K array. S: number of stack, K: number of x labels
    xname: name of xaxis categories (put together one) size K
    stackname: name of groups (size K)
    """
    # Set position of bar on X axis
    
    if isinstance(data, np.ndarray): data = data.tolist()
    S = len(data)
    K = len(data[0])
    zeros = [0 for _ in range(K)]
    data.insert(0, zeros)
    sums = [np.sum(data[:i], axis=0).tolist() for i in range(1, S+1)] # bottom requires total height

    for idx, sn,summ in zip(range(1, S+1), stackname, sums):
        datus = data[idx]
        plt.bar(xname, datus, bottom=summ, label=sn, width=0.5)
    
    # Add xticks on the middle of the group bars
    plt.xlabel('years', fontweight='bold')
    
    # Create legend & Show graphic
    plt.legend()
    if savefig:
        plt.savefig(savefig)
        plt.close()
    elif show:
        plt.show()
