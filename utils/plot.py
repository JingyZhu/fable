"""
Plot utilities
"""
from matplotlib import pyplot as plt


def plot_CDF(data, classname=[], savefig=''):
    """
    Plot the CDF for different class
    data should be a 2-dimensional list with each row a label
    """
    data = [sorted(datus) for datus in data]
    if len(classname) != len(data):
        classname = [ str(i + 1) for i in range(len(data))]
    for datus, cn in zip(data, classname):
        length = len(datus)
        x = datus
        y = [ (i + 1) / length for i in range(length)]
        plt.plot(x, y, label=cn)
    if savefig:
        plt.savefig(savefig)
    else:
        plt.show()

